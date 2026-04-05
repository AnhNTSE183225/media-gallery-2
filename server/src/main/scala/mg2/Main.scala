package mg2

import cats.effect.IO
import cats.effect.IOApp
import com.comcast.ip4s.Host
import com.comcast.ip4s.Port
import io.circe.Decoder
import io.circe.Encoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import io.circe.yaml.parser as yamlParser
import fs2.Stream
import fs2.io.file.Path as Fs2Path
import org.http4s.HttpRoutes
import org.http4s.Header
import org.http4s.StaticFile
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.io._
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits._
import org.typelevel.ci.CIString

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.AtomicMoveNotSupportedException
import java.security.MessageDigest
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

final case class BootstrapStatus(
    status: String,
    profile: String,
    startedAt: Option[String],
    finishedAt: Option[String],
    message: String,
    error: Option[String]
)

final case class ScanStatus(
    status: String,
    profile: String,
    processedArtists: Int,
    totalArtists: Int,
    percentage: Int,
    currentArtist: String,
    recentLogs: List[String],
    startedAt: Option[String],
    finishedAt: Option[String],
    error: Option[String]
)

final case class ProfileConfig(
    rootDirectory: String,
    allowedTags: List[String],
    allowedExtensions: List[String]
)

final case class KeybindConfig(previous: List[String], next: List[String], close: List[String])
final case class ViewerConfig(videoSkipSeconds: Double, keybinds: KeybindConfig)
final case class SearchConfig(defaultItemsPerPage: Int, maxItemsPerPage: Int)
final case class ProfilesConfig(activeProfile: String, list: Map[String, ProfileConfig])
final case class ServerConfig(port: Int)
final case class DatabaseConfig(host: String, port: Int, name: String, user: String, password: String)

final case class AppConfig(
    server: ServerConfig,
    database: DatabaseConfig,
    search: SearchConfig,
    viewer: ViewerConfig,
    profiles: ProfilesConfig
)

final case class ProfileView(
    name: String,
    rootDirectory: String,
    allowedTags: List[String],
    allowedExtensions: List[String]
)

final case class ProfilesResponse(activeProfile: String, profiles: List[ProfileView])
final case class SwitchProfileRequest(name: String)
final case class SwitchProfileResponse(success: Boolean, activeProfile: String)
final case class ScanTriggerResponse(success: Boolean, runId: String)
final case class ErrorResponse(error: String, code: String)

final case class AppConfigResponse(
    itemsPerPage: Int,
    videoSkipSeconds: Double,
    keybinds: KeybindConfig
)

final case class Pagination(total: Int, page: Int, limit: Int, totalPages: Int)
final case class AssetRow(
    id: String,
    `type`: String,
    artist: String,
    name: String,
    path: String,
    pages: Option[List[String]],
    tags: List[String]
)
final case class AssetsResponse(items: List[AssetRow], pagination: Pagination)

final case class DbProfile(
    id: Long,
    name: String,
    rootDirectory: String,
    allowedTags: List[String],
    allowedExtensions: List[String],
    isActive: Boolean
)

final case class ScanRow(
    status: String,
    processedArtists: Int,
    totalArtists: Int,
    currentArtist: String,
    error: Option[String],
    startedAt: Option[String],
    finishedAt: Option[String]
)

final case class FilterToken(kind: String, tags: List[String])
final case class IndexedAsset(artist: String, name: String, path: String, tags: List[String], kind: Int, pages: Option[List[String]])
final case class StoryPage(relativePath: String, absolutePath: String, fileName: String)

object Main extends IOApp.Simple {

  private given Encoder[BootstrapStatus] = deriveEncoder
  private given Encoder[ScanStatus] = deriveEncoder
  private given Encoder[ProfileView] = deriveEncoder
  private given Encoder[ProfilesResponse] = deriveEncoder
  private given Decoder[SwitchProfileRequest] = deriveDecoder
  private given Encoder[SwitchProfileResponse] = deriveEncoder
  private given Encoder[ScanTriggerResponse] = deriveEncoder
  private given Encoder[ErrorResponse] = deriveEncoder
  private given Encoder[KeybindConfig] = deriveEncoder
  private given Encoder[AppConfigResponse] = deriveEncoder
  private given Encoder[Pagination] = deriveEncoder
  private given Encoder[AssetRow] = deriveEncoder
  private given Encoder[AssetsResponse] = deriveEncoder

  private given Decoder[KeybindConfig] = deriveDecoder
  private given Decoder[ViewerConfig] = deriveDecoder
  private given Decoder[SearchConfig] = deriveDecoder
  private given Decoder[ProfileConfig] = deriveDecoder
  private given Decoder[ProfilesConfig] = deriveDecoder
  private given Decoder[ServerConfig] = deriveDecoder
  private given Decoder[DatabaseConfig] = deriveDecoder
  private given Decoder[AppConfig] = deriveDecoder

  private object PageParam extends OptionalQueryParamDecoderMatcher[Int]("page")
  private object LimitParam extends OptionalQueryParamDecoderMatcher[Int]("limit")
  private object QueryParam extends OptionalQueryParamDecoderMatcher[String]("q")
  private object TextParam extends OptionalQueryParamDecoderMatcher[String]("text")
  private object MediaPathParam extends OptionalQueryParamDecoderMatcher[String]("path")
  private object ThumbnailParam extends OptionalQueryParamDecoderMatcher[String]("thumbnail")

  private val bootStartedAt = Instant.now().toString
  private val digitPattern: Regex = "[0-9]+".r
  private val thumbnailCacheDirectory = Path.of(".cache", "thumbnails")
  private val thumbnailCaptureSecond = 1
  private val thumbnailWidth = 480
  private val thumbnailHeight = 640
  private val videoExtensions = Set(".mp4", ".webm", ".mov", ".m4v", ".mkv", ".avi")
  private val imageExtensions = Set(".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")

  private def nowIso: String = Instant.now().toString

  private def isTruthy(raw: String): Boolean =
    raw.trim.toLowerCase match {
      case "1" | "true" | "yes" | "on" => true
      case _                                => false
    }

  private def fileExtension(path: Path): String = {
    val name = path.getFileName.toString
    val dot = name.lastIndexOf('.')
    if (dot >= 0) name.substring(dot).toLowerCase else ""
  }

  private def isVideoFile(path: Path): Boolean =
    videoExtensions.contains(fileExtension(path))

  private def isImageFile(path: Path): Boolean =
    imageExtensions.contains(fileExtension(path))

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8))
    bytes.map("%02x".format(_)).mkString
  }

  private def cacheVideoThumbnail(source: Path): IO[Option[Path]] =
    IO.blocking {
      Files.createDirectories(thumbnailCacheDirectory)
      val attrs = Files.readAttributes(source, classOf[BasicFileAttributes])
      val cacheKey = sha256Hex(s"${source.toAbsolutePath.normalize()}|${attrs.lastModifiedTime().toMillis}|${attrs.size()}")
      val target = thumbnailCacheDirectory.resolve(s"$cacheKey.jpg")

      if (Files.exists(target) && Files.size(target) > 0) {
        Logger.debug(s"[Media] video thumbnail cache hit: source=${source.toAbsolutePath.normalize()} target=$target")
        Some(target)
      } else {
        Logger.debug(s"[Media] video thumbnail cache miss: source=${source.toAbsolutePath.normalize()} target=$target")
        def captureAt(second: Int): Boolean = {
          val temp = thumbnailCacheDirectory.resolve(s"$cacheKey-${UUID.randomUUID()}.tmp.jpg")
          val command = List(
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-ss",
            second.toString,
            "-i",
            source.toString,
            "-frames:v",
            "1",
            "-vf",
            s"scale=$thumbnailWidth:-1:force_original_aspect_ratio=decrease",
            temp.toString
          )
          val process = new ProcessBuilder(command*).start()
          val exited = process.waitFor()
          if (exited == 0 && Files.exists(temp) && Files.size(temp) > 0) {
            try {
              Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch {
              case _: AtomicMoveNotSupportedException =>
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
            }
            Logger.debug(s"[Media] video thumbnail generated: source=${source.toAbsolutePath.normalize()} second=$second target=$target")
            true
          } else {
            if (Files.exists(temp)) Files.delete(temp)
            Logger.debug(s"[Media] video thumbnail generation failed: source=${source.toAbsolutePath.normalize()} second=$second exitCode=$exited")
            false
          }
        }

        if (captureAt(thumbnailCaptureSecond) || captureAt(0)) Some(target)
        else None
      }
    }

  private def cacheImageThumbnail(source: Path): IO[Option[Path]] =
    IO.blocking {
      Files.createDirectories(thumbnailCacheDirectory)
      val attrs = Files.readAttributes(source, classOf[BasicFileAttributes])
      val cacheKey = sha256Hex(s"img|${source.toAbsolutePath.normalize()}|${attrs.lastModifiedTime().toMillis}|${attrs.size()}")
      val target = thumbnailCacheDirectory.resolve(s"$cacheKey.jpg")

      if (Files.exists(target) && Files.size(target) > 0) {
        Logger.debug(s"[Media] image thumbnail cache hit: source=${source.toAbsolutePath.normalize()} target=$target")
        Some(target)
      } else {
        Logger.debug(s"[Media] image thumbnail cache miss: source=${source.toAbsolutePath.normalize()} target=$target")
        val sourceImage = ImageIO.read(source.toFile)
        if (sourceImage == null || sourceImage.getWidth <= 0 || sourceImage.getHeight <= 0) {
          Logger.debug(s"[Media] image thumbnail decode failed: source=${source.toAbsolutePath.normalize()}")
          None
        } else {
          val widthScale = thumbnailWidth.toDouble / sourceImage.getWidth.toDouble
          val heightScale = thumbnailHeight.toDouble / sourceImage.getHeight.toDouble
          val scale = Math.min(widthScale, heightScale)
          val targetWidth = Math.max(1, Math.round(sourceImage.getWidth * scale).toInt)
          val targetHeight = Math.max(1, Math.round(sourceImage.getHeight * scale).toInt)

          val resized = new BufferedImage(targetWidth, targetHeight, BufferedImage.TYPE_INT_RGB)
          val graphics = resized.createGraphics()
          try {
            graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
            graphics.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
            graphics.drawImage(sourceImage, 0, 0, targetWidth, targetHeight, null)
          } finally {
            graphics.dispose()
          }

          val temp = thumbnailCacheDirectory.resolve(s"$cacheKey-${UUID.randomUUID()}.tmp.jpg")
          val wrote = ImageIO.write(resized, "jpg", temp.toFile)
          if (!wrote || !Files.exists(temp) || Files.size(temp) <= 0) {
            if (Files.exists(temp)) Files.delete(temp)
            Logger.debug(s"[Media] image thumbnail write failed: source=${source.toAbsolutePath.normalize()}")
            None
          } else {
            try {
              Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch {
              case _: AtomicMoveNotSupportedException =>
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
            }
            Logger.debug(s"[Media] image thumbnail generated: source=${source.toAbsolutePath.normalize()} target=$target size=${targetWidth}x${targetHeight}")
            Some(target)
          }
        }
      }
    }

  private def naturalKey(value: String): String =
    digitPattern.replaceAllIn(value.toLowerCase, matched =>
      val digits = matched.matched.dropWhile(_ == '0')
      val normalized = if digits.isEmpty then "0" else digits
      val padding = Math.max(0, 40 - normalized.length)
      "0" * padding + normalized
    )

  private def isAllowedTag(folder: String, allowedTagSet: Set[String]): Boolean =
    allowedTagSet.contains(folder.toLowerCase)

  private def loadConfig: IO[AppConfig] =
    IO.blocking(Files.readString(Path.of("config.yaml"))).flatMap { yaml =>
      IO.fromEither(yamlParser.parse(yaml)).flatMap { json =>
        IO.fromEither(json.as[AppConfig])
      }
    }

  private def clamp(value: Int, min: Int, max: Int): Int =
    Math.max(min, Math.min(max, value))

  private def parseTokens(raw: Option[String]): List[FilterToken] =
    raw.toList
      .flatMap(_.split(",").toList)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { token =>
        if (token.startsWith("-")) FilterToken("not", List(token.drop(1).trim.toLowerCase).filter(_.nonEmpty))
        else if (token.contains("|")) {
          val tags = token.split("\\|").toList.map(_.trim.toLowerCase).filter(_.nonEmpty)
          FilterToken("or", tags)
        } else FilterToken("and", List(token.toLowerCase))
      }
      .filter(_.tags.nonEmpty)

  private object Db {

    private def jdbcUrl(cfg: DatabaseConfig): String =
      s"jdbc:postgresql://${cfg.host}:${cfg.port}/${cfg.name}"

    private def withConnection[A](cfg: DatabaseConfig)(f: Connection => A): IO[A] =
      IO.blocking {
        Class.forName("org.postgresql.Driver")
        val conn = DriverManager.getConnection(jdbcUrl(cfg), cfg.user, cfg.password)
        try f(conn)
        finally conn.close()
      }

    private def setParams(ps: PreparedStatement, params: List[Any]): Unit = {
      var idx = 1
      params.foreach { p =>
        p match {
          case i: Int    => ps.setInt(idx, i)
          case l: Long   => ps.setLong(idx, l)
          case s: String => ps.setString(idx, s)
          case b: Boolean => ps.setBoolean(idx, b)
          case null      => ps.setObject(idx, null)
          case other     => ps.setObject(idx, other)
        }
        idx += 1
      }
    }

    private def splitSqlStatements(sql: String): List[String] =
      sql
        .split(';')
        .toList
        .map(_.trim)
        .filter(_.nonEmpty)

    def initSchema(cfg: DatabaseConfig): IO[Unit] =
      for {
        sql <- IO.blocking(Files.readString(Path.of("sql", "001_init.sql")))
        statements = splitSqlStatements(sql)
        _ <- withConnection(cfg) { conn =>
          conn.setAutoCommit(false)
          try {
            statements.foreach(conn.createStatement().execute)
            conn.commit()
          } catch {
            case e: Throwable =>
              conn.rollback()
              throw e
          }
        }
      } yield ()

    def syncProfiles(cfg: DatabaseConfig, profilesCfg: ProfilesConfig): IO[Unit] =
      withConnection(cfg) { conn =>
        conn.setAutoCommit(false)
        try {
          profilesCfg.list.foreach { case (name, profile) =>
            val upsert = conn.prepareStatement(
              """
                |INSERT INTO profiles (name, root_directory, is_active)
                |VALUES (?, ?, FALSE)
                |ON CONFLICT (name)
                |DO UPDATE SET root_directory = EXCLUDED.root_directory
                |""".stripMargin
            )
            upsert.setString(1, name)
            upsert.setString(2, profile.rootDirectory)
            upsert.executeUpdate()
            upsert.close()

            val profileIdStmt = conn.prepareStatement("SELECT id FROM profiles WHERE name = ?")
            profileIdStmt.setString(1, name)
            val profileIdRs = profileIdStmt.executeQuery()
            profileIdRs.next()
            val profileId = profileIdRs.getLong(1)
            profileIdRs.close()
            profileIdStmt.close()

            val delTags = conn.prepareStatement("DELETE FROM profile_allowed_tags WHERE profile_id = ?")
            delTags.setLong(1, profileId)
            delTags.executeUpdate()
            delTags.close()

            val addTag = conn.prepareStatement("INSERT INTO profile_allowed_tags (profile_id, tag) VALUES (?, ?)")
            profile.allowedTags.foreach { tag =>
              addTag.setLong(1, profileId)
              addTag.setString(2, tag)
              addTag.addBatch()
            }
            addTag.executeBatch()
            addTag.close()

            val delExt = conn.prepareStatement("DELETE FROM profile_allowed_extensions WHERE profile_id = ?")
            delExt.setLong(1, profileId)
            delExt.executeUpdate()
            delExt.close()

            val addExt = conn.prepareStatement("INSERT INTO profile_allowed_extensions (profile_id, extension) VALUES (?, ?)")
            profile.allowedExtensions.foreach { ext =>
              addExt.setLong(1, profileId)
              addExt.setString(2, ext)
              addExt.addBatch()
            }
            addExt.executeBatch()
            addExt.close()
          }

          val setInactive = conn.prepareStatement("UPDATE profiles SET is_active = FALSE")
          setInactive.executeUpdate()
          setInactive.close()

          val setActive = conn.prepareStatement("UPDATE profiles SET is_active = TRUE WHERE name = ?")
          setActive.setString(1, profilesCfg.activeProfile)
          val updated = setActive.executeUpdate()
          setActive.close()

          if (updated != 1) throw new IllegalStateException("Configured active profile not found after sync")

          conn.commit()
        } catch {
          case e: Throwable =>
            conn.rollback()
            throw e
        }
      }

    def listProfiles(cfg: DatabaseConfig): IO[List[DbProfile]] =
      withConnection(cfg) { conn =>
        val sql =
          """
            |SELECT p.id, p.name, p.root_directory, p.is_active,
            |       COALESCE(array_agg(DISTINCT t.tag) FILTER (WHERE t.tag IS NOT NULL), '{}') AS tags,
            |       COALESCE(array_agg(DISTINCT e.extension) FILTER (WHERE e.extension IS NOT NULL), '{}') AS exts
            |FROM profiles p
            |LEFT JOIN profile_allowed_tags t ON t.profile_id = p.id
            |LEFT JOIN profile_allowed_extensions e ON e.profile_id = p.id
            |GROUP BY p.id
            |ORDER BY p.name ASC
            |""".stripMargin
        val ps = conn.prepareStatement(sql)
        val rs = ps.executeQuery()
        val rows = Iterator
          .continually(rs)
          .takeWhile(_.next())
          .map { r =>
            DbProfile(
              id = r.getLong("id"),
              name = r.getString("name"),
              rootDirectory = r.getString("root_directory"),
              allowedTags = r.getArray("tags").getArray.asInstanceOf[Array[Any]].toList.map(_.toString),
              allowedExtensions = r.getArray("exts").getArray.asInstanceOf[Array[Any]].toList.map(_.toString),
              isActive = r.getBoolean("is_active")
            )
          }
          .toList
        rs.close()
        ps.close()
        rows
      }

    def activeProfile(cfg: DatabaseConfig): IO[DbProfile] =
      listProfiles(cfg).flatMap { profiles =>
        profiles.find(_.isActive) match {
          case Some(profile) => IO.pure(profile)
          case None          => IO.raiseError(new IllegalStateException("No active profile in database"))
        }
      }

    def hasRunningScan(cfg: DatabaseConfig): IO[Boolean] =
      withConnection(cfg) { conn =>
        val ps = conn.prepareStatement("SELECT EXISTS (SELECT 1 FROM scan_runs WHERE status = 'running')")
        val rs = ps.executeQuery()
        rs.next()
        val running = rs.getBoolean(1)
        rs.close()
        ps.close()
        running
      }

    def switchActiveProfile(cfg: DatabaseConfig, name: String): IO[Either[String, Unit]] =
      withConnection(cfg) { conn =>
        conn.setAutoCommit(false)
        try {
          val check = conn.prepareStatement("SELECT EXISTS(SELECT 1 FROM profiles WHERE name = ?)")
          check.setString(1, name)
          val checkRs = check.executeQuery()
          checkRs.next()
          val exists = checkRs.getBoolean(1)
          checkRs.close()
          check.close()

          if (!exists) {
            conn.rollback()
            Left("unknown_profile")
          } else {
            conn.prepareStatement("UPDATE profiles SET is_active = FALSE").executeUpdate()
            val setActive = conn.prepareStatement("UPDATE profiles SET is_active = TRUE WHERE name = ?")
            setActive.setString(1, name)
            setActive.executeUpdate()
            setActive.close()
            conn.commit()
            Right(())
          }
        } catch {
          case e: Throwable =>
            conn.rollback()
            throw e
        }
      }

    def triggerScan(cfg: DatabaseConfig): IO[String] =
      withConnection(cfg) { conn =>
        conn.setAutoCommit(false)
        try {
          val profileStmt = conn.prepareStatement(
            """
              |SELECT p.id, p.root_directory,
              |       COALESCE(array_agg(DISTINCT e.extension) FILTER (WHERE e.extension IS NOT NULL), '{}') AS exts,
              |       COALESCE(array_agg(DISTINCT t.tag) FILTER (WHERE t.tag IS NOT NULL), '{}') AS tags
              |FROM profiles p
              |LEFT JOIN profile_allowed_extensions e ON e.profile_id = p.id
              |LEFT JOIN profile_allowed_tags t ON t.profile_id = p.id
              |WHERE p.is_active = TRUE
              |GROUP BY p.id
              |LIMIT 1
              |""".stripMargin
          )
          val profileRs = profileStmt.executeQuery()
          if (!profileRs.next()) throw new IllegalStateException("No active profile")

          val profileId = profileRs.getLong("id")
          val rootDirectory = profileRs.getString("root_directory")
          val allowedExtensions = profileRs.getArray("exts").getArray.asInstanceOf[Array[Any]].toList.map(_.toString.toLowerCase)
          val allowedTags = profileRs.getArray("tags").getArray.asInstanceOf[Array[Any]].toList.map(_.toString.toLowerCase)
          profileRs.close()
          profileStmt.close()

          val indexedAssets = collectAssets(rootDirectory, allowedExtensions, allowedTags)
          val distinctArtists = indexedAssets.map(_.artist).distinct.size

          val runId = UUID.randomUUID().toString
          val now = Timestamp.from(Instant.now())

          val insert = conn.prepareStatement(
            """
              |INSERT INTO scan_runs (profile_id, trigger, status, processed_artists, total_artists, current_artist, started_at)
              |VALUES (?, 'manual', 'running', 0, 0, '', ?)
              |""".stripMargin,
            java.sql.Statement.RETURN_GENERATED_KEYS
          )
          insert.setLong(1, profileId)
          insert.setTimestamp(2, now)
          insert.executeUpdate()
          val runKeys = insert.getGeneratedKeys
          if (!runKeys.next()) throw new IllegalStateException("Failed to create scan run")
          val runDbId = runKeys.getLong(1)
          runKeys.close()
          insert.close()

          val deleteTags = conn.prepareStatement(
            "DELETE FROM asset_tags WHERE asset_id IN (SELECT id FROM assets WHERE profile_id = ?)"
          )
          deleteTags.setLong(1, profileId)
          deleteTags.executeUpdate()
          deleteTags.close()

          val deleteAssets = conn.prepareStatement("DELETE FROM assets WHERE profile_id = ?")
          deleteAssets.setLong(1, profileId)
          deleteAssets.executeUpdate()
          deleteAssets.close()

          val insertAsset = conn.prepareStatement(
            """
              |INSERT INTO assets (profile_id, kind, artist, name, source_path, story_pages, mime, byte_size)
              |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              |""".stripMargin,
            java.sql.Statement.RETURN_GENERATED_KEYS
          )

          val insertTag = conn.prepareStatement(
            "INSERT INTO asset_tags (asset_id, tag) VALUES (?, ?)"
          )

          indexedAssets.foreach { asset =>
            insertAsset.setLong(1, profileId)
            insertAsset.setInt(2, asset.kind)
            insertAsset.setString(3, asset.artist)
            insertAsset.setString(4, asset.name)
            insertAsset.setString(5, asset.path)
            asset.pages match {
              case Some(pages) if pages.nonEmpty =>
                insertAsset.setArray(6, conn.createArrayOf("text", pages.map(_.asInstanceOf[Object]).toArray))
              case _ =>
                insertAsset.setNull(6, java.sql.Types.ARRAY)
            }
            insertAsset.setNull(7, java.sql.Types.VARCHAR)
            insertAsset.setNull(8, java.sql.Types.BIGINT)
            insertAsset.executeUpdate()

            val keys = insertAsset.getGeneratedKeys
            keys.next()
            val assetId = keys.getLong(1)
            keys.close()

            asset.tags.foreach { tag =>
              insertTag.setLong(1, assetId)
              insertTag.setString(2, tag)
              insertTag.addBatch()
            }
          }

          insertTag.executeBatch()
          insertTag.close()
          insertAsset.close()

          val complete = conn.prepareStatement(
            """
              |UPDATE scan_runs
              |SET status = 'complete',
              |    processed_artists = ?,
              |    total_artists = ?,
              |    current_artist = '',
              |    finished_at = ?
              |WHERE id = ?
              |""".stripMargin
          )
          complete.setInt(1, distinctArtists)
          complete.setInt(2, distinctArtists)
          complete.setTimestamp(3, now)
          complete.setLong(4, runDbId)
          complete.executeUpdate()
          complete.close()

          conn.commit()
          runId
        } catch {
          case e: Throwable =>
            conn.rollback()
            throw e
        }
      }

    private def collectAssets(rootDirectory: String, allowedExtensions: List[String], allowedTags: List[String]): List[IndexedAsset] = {
      val root = Path.of(rootDirectory)
      if (!Files.exists(root) || !Files.isDirectory(root)) {
        return Nil
      }

      final case class StoryDraft(
          artist: String,
          name: String,
          tags: List[String],
          pages: scala.collection.mutable.ListBuffer[StoryPage]
      )

      val extensionSet = allowedExtensions.map(_.toLowerCase).toSet
      val allowedTagSet = allowedTags.map(_.toLowerCase).toSet
      val standaloneAssets = scala.collection.mutable.ListBuffer.empty[IndexedAsset]
      val storyAssets = scala.collection.mutable.LinkedHashMap.empty[String, StoryDraft]

      val stream = Files.walk(root)
      try {
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path))
          .filter { path =>
            val file = path.getFileName.toString
            val dot = file.lastIndexOf('.')
            dot >= 0 && extensionSet.contains(file.substring(dot).toLowerCase)
          }
          .foreach { path =>
            val relative = root.relativize(path)
            val parts = relative.iterator().asScala.map(_.toString).toList
            val artist = parts.headOption.getOrElse("Unknown")
            val fileName = path.getFileName.toString
            val dot = fileName.lastIndexOf('.')
            val name = if (dot > 0) fileName.substring(0, dot) else fileName
            val folders = parts.dropRight(1)
            val artistFolders = folders.drop(1)

            val firstStoryIndex = artistFolders.indexWhere(folder => !isAllowedTag(folder, allowedTagSet))
            if (firstStoryIndex < 0) {
              val tags = artistFolders.map(_.toLowerCase).distinct
              standaloneAssets += IndexedAsset(
                artist = artist,
                name = name,
                path = path.toAbsolutePath.normalize().toString,
                tags = tags,
                kind = 1,
                pages = None
              )
            } else {
              val storyName = artistFolders(firstStoryIndex)
              val storyTags = artistFolders.take(firstStoryIndex).map(_.toLowerCase).distinct :+ "story"
              val storyKey = (artist :: artistFolders.take(firstStoryIndex + 1)).mkString("/")
              val draft = storyAssets.getOrElseUpdate(
                storyKey,
                StoryDraft(
                  artist = artist,
                  name = storyName,
                  tags = storyTags.distinct,
                  pages = scala.collection.mutable.ListBuffer.empty[StoryPage]
                )
              )
              draft.pages += StoryPage(
                relativePath = root.relativize(path).toString,
                absolutePath = path.toAbsolutePath.normalize().toString,
                fileName = fileName
              )
            }
          }

        val storyIndexedAssets = storyAssets.values.toList.map { draft =>
          val orderedPages = draft.pages.toList
            .sortBy(page => (naturalKey(page.fileName), naturalKey(page.relativePath)))
            .map(_.absolutePath)
          IndexedAsset(
            artist = draft.artist,
            name = draft.name,
            path = orderedPages.headOption.getOrElse(""),
            tags = draft.tags,
            kind = 2,
            pages = Some(orderedPages)
          )
        }

        (standaloneAssets.toList ++ storyIndexedAssets)
          .sortBy(asset => (naturalKey(asset.artist), naturalKey(asset.name), naturalKey(asset.path)))
      } finally {
        stream.close()
      }
    }

    def latestScan(cfg: DatabaseConfig): IO[Option[ScanRow]] =
      withConnection(cfg) { conn =>
        val sql =
          """
            |SELECT sr.status, sr.processed_artists, sr.total_artists, sr.current_artist,
            |       sr.error_message, sr.started_at, sr.finished_at
            |FROM scan_runs sr
            |JOIN profiles p ON p.id = sr.profile_id
            |WHERE p.is_active = TRUE
            |ORDER BY sr.started_at DESC
            |LIMIT 1
            |""".stripMargin
        val ps = conn.prepareStatement(sql)
        val rs = ps.executeQuery()
        val row =
          if (rs.next()) {
            Some(
              ScanRow(
                status = rs.getString("status"),
                processedArtists = rs.getInt("processed_artists"),
                totalArtists = rs.getInt("total_artists"),
                currentArtist = rs.getString("current_artist"),
                error = Option(rs.getString("error_message")),
                startedAt = Option(rs.getTimestamp("started_at")).map(_.toInstant.toString),
                finishedAt = Option(rs.getTimestamp("finished_at")).map(_.toInstant.toString)
              )
            )
          } else None
        rs.close()
        ps.close()
        row
      }

    def searchAssets(cfg: DatabaseConfig, text: Option[String], q: Option[String], page: Int, limit: Int): IO[(Int, List[AssetRow])] =
      withConnection(cfg) { conn =>
        val active = conn.prepareStatement("SELECT id FROM profiles WHERE is_active = TRUE LIMIT 1")
        val activeRs = active.executeQuery()
        val maybeProfileId = if (activeRs.next()) Some(activeRs.getLong(1)) else None
        activeRs.close()
        active.close()

        maybeProfileId match {
          case None =>
            (0, Nil)
          case Some(profileId) =>
            val tokens = parseTokens(q)

            val conditions = scala.collection.mutable.ListBuffer[String]("a.profile_id = ?")
            val args = scala.collection.mutable.ListBuffer[Any](profileId)

            text.map(_.trim).filter(_.nonEmpty).foreach { t =>
              conditions += "(a.artist || ' ' || a.name) ILIKE ?"
              args += s"%$t%"
            }

            tokens.foreach { token =>
              token.kind match {
                case "and" =>
                  conditions += "EXISTS (SELECT 1 FROM asset_tags at1 WHERE at1.asset_id = a.id AND lower(at1.tag) = ?)"
                  args += token.tags.head
                case "not" =>
                  conditions += "NOT EXISTS (SELECT 1 FROM asset_tags at2 WHERE at2.asset_id = a.id AND lower(at2.tag) = ?)"
                  args += token.tags.head
                case "or" =>
                  val placeholders = token.tags.map(_ => "?").mkString(",")
                  conditions += s"EXISTS (SELECT 1 FROM asset_tags at3 WHERE at3.asset_id = a.id AND lower(at3.tag) IN ($placeholders))"
                  args ++= token.tags
                case _ =>
              }
            }

            val whereSql = conditions.mkString(" WHERE ", " AND ", "")

            val countSql = s"SELECT count(*) FROM assets a$whereSql"
            val countPs = conn.prepareStatement(countSql)
            setParams(countPs, args.toList)
            val countRs = countPs.executeQuery()
            countRs.next()
            val total = countRs.getInt(1)
            countRs.close()
            countPs.close()

            val rowsSql =
              s"""
                 |SELECT a.id, a.kind, a.artist, a.name, a.source_path, a.story_pages,
                 |       COALESCE(array_agg(t.tag ORDER BY lower(t.tag)) FILTER (WHERE t.tag IS NOT NULL), '{}') AS tags
                 |FROM assets a
                 |LEFT JOIN asset_tags t ON t.asset_id = a.id
                 |$whereSql
                 |GROUP BY a.id
                 |ORDER BY
                 |  (SELECT string_agg(CASE WHEN m[1] ~ '^[0-9]+$$' THEN lpad(m[1], 40, '0') ELSE lower(m[1]) END, '')
                 |   FROM regexp_matches(a.artist, '([0-9]+|[^0-9]+)', 'g') AS m),
                 |  (SELECT string_agg(CASE WHEN m[1] ~ '^[0-9]+$$' THEN lpad(m[1], 40, '0') ELSE lower(m[1]) END, '')
                 |   FROM regexp_matches(a.name, '([0-9]+|[^0-9]+)', 'g') AS m),
                 |  lower(a.source_path)
                 |LIMIT ? OFFSET ?
                 |""".stripMargin

            val rowsPs = conn.prepareStatement(rowsSql)
            setParams(rowsPs, args.toList ++ List(limit, (page - 1) * limit))
            val rs = rowsPs.executeQuery()
            val items = Iterator
              .continually(rs)
              .takeWhile(_.next())
              .map { r =>
                val pagesArray = r.getArray("story_pages")
                val pages = if (pagesArray == null) None else Some(pagesArray.getArray.asInstanceOf[Array[Any]].toList.map(_.toString))
                AssetRow(
                  id = r.getLong("id").toString,
                  `type` = if (r.getInt("kind") == 2) "story" else "image",
                  artist = r.getString("artist"),
                  name = r.getString("name"),
                  path = r.getString("source_path"),
                  pages = pages,
                  tags = r.getArray("tags").getArray.asInstanceOf[Array[Any]].toList.map(_.toString)
                )
              }
              .toList
            rs.close()
            rowsPs.close()

            (total, items)
        }
      }

    def profileRootForPath(cfg: DatabaseConfig, rawPath: String): IO[Option[Path]] =
      activeProfile(cfg).map { profile =>
        val root = Path.of(profile.rootDirectory).toAbsolutePath.normalize()
        val candidate = Path.of(rawPath).toAbsolutePath.normalize()
        if (candidate.startsWith(root)) Some(candidate) else None
      }
  }

  private def routes(config: AppConfig): HttpRoutes[IO] =
    HttpRoutes.of[IO] {
      case GET -> Root / "v1" / "health" =>
        Ok("ok")

      case GET -> Root / "v1" / "profiles" =>
        Db.listProfiles(config.database).flatMap { profiles =>
          val active = profiles.find(_.isActive).map(_.name).getOrElse("")
          val payload = profiles.map { p =>
            ProfileView(p.name, p.rootDirectory, p.allowedTags.sorted, p.allowedExtensions.sorted)
          }
          Ok(ProfilesResponse(active, payload))
        }

      case req @ PUT -> Root / "v1" / "profiles" / "active" =>
        req.as[SwitchProfileRequest].flatMap { body =>
          Db.hasRunningScan(config.database).flatMap {
            case true => Conflict(ErrorResponse("Scan already running", "scan_running"))
            case false =>
              Db.switchActiveProfile(config.database, body.name).flatMap {
                case Left("unknown_profile") => BadRequest(ErrorResponse("Unknown profile", "unknown_profile"))
                case Left(_)                  => BadRequest(ErrorResponse("Profile switch failed", "profile_switch_failed"))
                case Right(_)                 => Ok(SwitchProfileResponse(success = true, activeProfile = body.name))
              }
          }
        }

      case GET -> Root / "v1" / "bootstrap-status" =>
        Db.activeProfile(config.database).flatMap { active =>
          Ok(
            BootstrapStatus(
              status = "ready",
              profile = active.name,
              startedAt = Some(bootStartedAt),
              finishedAt = Some(nowIso),
              message = "Bootstrap completed",
              error = None
            )
          )
        }

      case GET -> Root / "v1" / "scan-status" =>
        for {
          active <- Db.activeProfile(config.database)
          latest <- Db.latestScan(config.database)
          payload = latest match {
            case Some(row) =>
              val percentage = if (row.totalArtists > 0) ((row.processedArtists * 100) / row.totalArtists) else if (row.status == "complete") 100 else 0
              ScanStatus(
                status = row.status,
                profile = active.name,
                processedArtists = row.processedArtists,
                totalArtists = row.totalArtists,
                percentage = percentage,
                currentArtist = row.currentArtist,
                recentLogs = List(s"[${nowIso}] last scan status: ${row.status}"),
                startedAt = row.startedAt,
                finishedAt = row.finishedAt,
                error = row.error
              )
            case None =>
              ScanStatus(
                status = "idle",
                profile = active.name,
                processedArtists = 0,
                totalArtists = 0,
                percentage = 0,
                currentArtist = "",
                recentLogs = Nil,
                startedAt = None,
                finishedAt = None,
                error = None
              )
          }
          res <- Ok(payload)
        } yield res

      case POST -> Root / "v1" / "scans" =>
        Db.hasRunningScan(config.database).flatMap {
          case true => Conflict(ErrorResponse("Scan already running", "scan_running"))
          case false =>
            Db.triggerScan(config.database).flatMap { runId =>
              Ok(ScanTriggerResponse(success = true, runId = runId))
            }
        }

      case GET -> Root / "v1" / "scan-events" =>
        for {
          active <- Db.activeProfile(config.database)
          latest <- Db.latestScan(config.database)
          payload = latest match {
            case Some(row) =>
              ScanStatus(
                status = row.status,
                profile = active.name,
                processedArtists = row.processedArtists,
                totalArtists = row.totalArtists,
                percentage = if (row.totalArtists > 0) ((row.processedArtists * 100) / row.totalArtists) else if (row.status == "complete") 100 else 0,
                currentArtist = row.currentArtist,
                recentLogs = List(s"[${nowIso}] scan-event snapshot"),
                startedAt = row.startedAt,
                finishedAt = row.finishedAt,
                error = row.error
              )
            case None =>
              ScanStatus(
                status = "idle",
                profile = active.name,
                processedArtists = 0,
                totalArtists = 0,
                percentage = 0,
                currentArtist = "",
                recentLogs = Nil,
                startedAt = None,
                finishedAt = None,
                error = None
              )
          }
          sseData = s"event: scan\\ndata: ${payload.asJson.noSpaces}\\n\\n"
          body = Stream.emit(sseData).through(fs2.text.utf8.encode).covary[IO]
          res <- Ok(body).map(
            _.putHeaders(
              Header.Raw(CIString("Content-Type"), "text/event-stream"),
              Header.Raw(CIString("Cache-Control"), "no-cache")
            )
          )
        } yield res

      case GET -> Root / "v1" / "assets" :? QueryParam(qOpt) +& TextParam(textOpt) +& PageParam(pageOpt) +& LimitParam(limitOpt) =>
        val page = pageOpt.filter(_ > 0).getOrElse(1)
        val limit = clamp(limitOpt.getOrElse(config.search.defaultItemsPerPage), 1, config.search.maxItemsPerPage)
        Db.searchAssets(config.database, textOpt, qOpt, page, limit).flatMap { case (total, items) =>
          val totalPages = if (total == 0) 0 else ((total + limit - 1) / limit)
          Ok(AssetsResponse(items, Pagination(total, page, limit, totalPages)))
        }

      case GET -> Root / "v1" / "app-config" =>
        val app = AppConfigResponse(
          itemsPerPage = config.search.defaultItemsPerPage,
          videoSkipSeconds = config.viewer.videoSkipSeconds,
          keybinds = config.viewer.keybinds
        )
        Ok(app)

      case req @ GET -> Root / "v1" / "media" :? MediaPathParam(pathOpt) +& ThumbnailParam(thumbnailOpt) =>
        val wantsThumbnail = thumbnailOpt.exists(isTruthy)
        val startedAt = System.nanoTime()
        pathOpt match {
          case None =>
            Logger.debug(s"[Media] request rejected: missing path thumbnail=$wantsThumbnail")
            BadRequest(ErrorResponse("Missing path query parameter", "missing_path"))
          case Some(rawPath) =>
            Logger.debug(s"[Media] request: path=$rawPath thumbnail=$wantsThumbnail")
            Db.profileRootForPath(config.database, rawPath).flatMap {
              case None =>
                Logger.debug(s"[Media] request forbidden: path=$rawPath")
                Forbidden(ErrorResponse("Path outside active profile root", "path_not_allowed"))
              case Some(file) =>
                if (!Files.exists(file) || !Files.isRegularFile(file)) {
                  Logger.debug(s"[Media] request missing file: path=$rawPath resolved=${file.toAbsolutePath.normalize()}")
                  NotFound(ErrorResponse("Media not found", "media_not_found"))
                } else if (wantsThumbnail && isImageFile(file)) {
                  cacheImageThumbnail(file).flatMap {
                    case Some(thumbnail) =>
                      val tookMs = (System.nanoTime() - startedAt) / 1000000
                      Logger.debug(s"[Media] response image-thumbnail: path=$rawPath tookMs=$tookMs")
                      StaticFile.fromPath(Fs2Path.fromNioPath(thumbnail), Some(req)).getOrElseF(NotFound())
                    case None =>
                      val tookMs = (System.nanoTime() - startedAt) / 1000000
                      Logger.debug(s"[Media] response image-thumbnail failed: path=$rawPath tookMs=$tookMs")
                      NotFound(ErrorResponse("Image thumbnail generation failed", "thumbnail_unavailable"))
                  }
                } else if (wantsThumbnail && isVideoFile(file)) {
                  cacheVideoThumbnail(file).flatMap {
                    case Some(thumbnail) =>
                      val tookMs = (System.nanoTime() - startedAt) / 1000000
                      Logger.debug(s"[Media] response video-thumbnail: path=$rawPath tookMs=$tookMs")
                      StaticFile.fromPath(Fs2Path.fromNioPath(thumbnail), Some(req)).getOrElseF(NotFound())
                    case None =>
                      val tookMs = (System.nanoTime() - startedAt) / 1000000
                      Logger.debug(s"[Media] response video-thumbnail failed: path=$rawPath tookMs=$tookMs")
                      NotFound(ErrorResponse("Video thumbnail generation failed", "thumbnail_unavailable"))
                  }
                } else {
                  val tookMs = (System.nanoTime() - startedAt) / 1000000
                  Logger.debug(s"[Media] response original-file: path=$rawPath tookMs=$tookMs")
                  StaticFile.fromPath(Fs2Path.fromNioPath(file), Some(req)).getOrElseF(NotFound())
                }
            }
        }
    }

  override def run: IO[Unit] =
    for {
      config <- loadConfig
      _ <- Db.initSchema(config.database)
      _ <- Db.syncProfiles(config.database, config.profiles)
      _ <- EmberServerBuilder
        .default[IO]
        .withHost(Host.fromString("0.0.0.0").getOrElse(Host.fromString("127.0.0.1").get))
        .withPort(Port.fromInt(config.server.port).getOrElse(Port.fromInt(3001).get))
        .withHttpApp(routes(config).orNotFound)
        .build
        .useForever
    } yield ()
}
