import { init } from "snabbdom/build/init.js";
import { classModule } from "snabbdom/build/modules/class.js";
import { eventListenersModule } from "snabbdom/build/modules/eventlisteners.js";
import { propsModule } from "snabbdom/build/modules/props.js";
import { styleModule } from "snabbdom/build/modules/style.js";
import { h } from "snabbdom/build/h.js";
import type { VNode } from "snabbdom/build/vnode.js";
import { logger } from "./logger";
import "./styles/app.scss";

type BootstrapStatus = {
  status: "pending" | "running" | "ready" | "error";
  profile: string;
  message: string;
};

type ScanStatus = {
  status: "idle" | "running" | "complete" | "error";
  profile: string;
  processedArtists: number;
  totalArtists: number;
  percentage: number;
  currentArtist: string;
  recentLogs: string[];
  error: string | null;
};

type Profile = {
  name: string;
  rootDirectory: string;
  allowedTags: string[];
  allowedExtensions: string[];
};

type ProfilesResponse = {
  activeProfile: string;
  profiles: Profile[];
};

type AppConfigResponse = {
  itemsPerPage: number;
  videoSkipSeconds: number;
  keybinds: {
    previous: string[];
    next: string[];
    close: string[];
  };
};

type Asset = {
  id: string;
  type: "image" | "story";
  artist: string;
  name: string;
  path: string;
  pages: string[] | null;
  tags: string[];
};

type AssetsResponse = {
  items: Asset[];
  pagination: {
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  };
};

type StartupState = {
  ready: boolean;
  message: string;
  attempts: number;
  checks: {
    bootstrap: boolean;
    profiles: boolean;
    appConfig: boolean;
    assets: boolean;
  };
};

type UiState = {
  bootstrap: BootstrapStatus;
  scan: ScanStatus;
  isScanning: boolean;
  activeProfile: string;
  profiles: Profile[];
  itemsPerPage: number;
  videoSkipSeconds: number;
  keybinds: AppConfigResponse["keybinds"];
  searchText: string;
  searchTags: string;
  page: number;
  totalPages: number;
  totalAssets: number;
  assets: Asset[];
  selectedAssetIndex: number;
  selectedStoryPageIndex: number;
  scanMessage: string;
  startup: StartupState;
};

const patch = init([classModule, propsModule, styleModule, eventListenersModule]);
const mount = document.getElementById("app");

if (!mount) {
  throw new Error("Missing #app mount node");
}

let vnode: VNode = mount as unknown as VNode;
let scanPollTimer: number | null = null;
let startupPollTimer: number | null = null;
let lastWheelAt = 0;

const state: UiState = {
  bootstrap: {
    status: "pending",
    profile: "",
    message: "Waiting for server bootstrap"
  },
  scan: {
    status: "idle",
    profile: "",
    processedArtists: 0,
    totalArtists: 0,
    percentage: 0,
    currentArtist: "",
    recentLogs: [],
    error: null
  },
  isScanning: false,
  activeProfile: "",
  profiles: [],
  itemsPerPage: 18,
  videoSkipSeconds: 3,
  keybinds: {
    previous: ["ArrowLeft"],
    next: ["ArrowRight"],
    close: ["Escape"]
  },
  searchText: "",
  searchTags: "",
  page: 1,
  totalPages: 0,
  totalAssets: 0,
  assets: [],
  selectedAssetIndex: -1,
  selectedStoryPageIndex: 0,
  scanMessage: "",
  startup: {
    ready: false,
    message: "Connecting to backend...",
    attempts: 0,
    checks: {
      bootstrap: false,
      profiles: false,
      appConfig: false,
      assets: false
    }
  }
};

function mediaUrl(path: string, thumbnail: boolean): string {
  return `/v1/media?path=${encodeURIComponent(path)}&thumbnail=${thumbnail ? "true" : "false"}`;
}

function isVideoPath(path: string): boolean {
  const lower = path.toLowerCase();
  return lower.endsWith(".mp4") || lower.endsWith(".webm");
}

function selectedAsset(model: UiState): Asset | null {
  if (model.selectedAssetIndex < 0 || model.selectedAssetIndex >= model.assets.length) {
    return null;
  }

  return model.assets[model.selectedAssetIndex] ?? null;
}

function selectedStoryPages(model: UiState): string[] {
  const asset = selectedAsset(model);
  if (!asset || !asset.pages || asset.pages.length === 0) {
    return [];
  }

  return asset.pages;
}

function selectedMediaPath(model: UiState, thumbnail: boolean): string {
  const asset = selectedAsset(model);
  if (!asset) {
    return "";
  }

  const pages = selectedStoryPages(model);
  const storyPage = pages.length > 0 ? pages[Math.min(model.selectedStoryPageIndex, pages.length - 1)] : null;
  return mediaUrl(storyPage ?? asset.path, thumbnail);
}

function selectedStoryLabel(model: UiState): string {
  const pages = selectedStoryPages(model);
  if (pages.length === 0) {
    return "";
  }

  return `${Math.min(model.selectedStoryPageIndex + 1, pages.length)}/${pages.length}`;
}

function isStoryAtStart(model: UiState): boolean {
  return model.selectedStoryPageIndex <= 0;
}

function isStoryAtEnd(model: UiState): boolean {
  const pages = selectedStoryPages(model);
  return pages.length > 0 && model.selectedStoryPageIndex >= pages.length - 1;
}

function applyUrlState(): void {
  const params = new URLSearchParams(window.location.search);
  state.searchText = params.get("text") ?? "";
  state.searchTags = params.get("q") ?? "";
  const page = Number(params.get("page") ?? "1");
  state.page = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
  const storyPage = Number(params.get("storyPage") ?? "0");
  state.selectedStoryPageIndex = Number.isFinite(storyPage) && storyPage >= 0 ? Math.floor(storyPage) : 0;
}

function syncUrlState(): void {
  const params = new URLSearchParams();
  if (state.searchText.trim()) params.set("text", state.searchText.trim());
  if (state.searchTags.trim()) params.set("q", state.searchTags.trim());
  if (state.page > 1) params.set("page", String(state.page));

  const selected = selectedAsset(state);
  if (selected) {
    params.set("view", selected.id);
    if (selected.pages && selected.pages.length > 0 && state.selectedStoryPageIndex > 0) {
      params.set("storyPage", String(state.selectedStoryPageIndex));
    }
  }

  const query = params.toString();
  const target = query ? `?${query}` : window.location.pathname;
  window.history.replaceState(null, "", target);
}

function setSelectedAssetById(assetId: string | null): void {
  if (!assetId) {
    state.selectedAssetIndex = -1;
    state.selectedStoryPageIndex = 0;
    return;
  }

  const index = state.assets.findIndex((asset) => asset.id === assetId);
  state.selectedAssetIndex = index;
  state.selectedStoryPageIndex = 0;
}

function selectAssetAt(index: number): void {
  if (index < 0 || index >= state.assets.length) return;
  state.selectedAssetIndex = index;
  state.selectedStoryPageIndex = 0;
  syncUrlState();
  render();
}

function closeViewer(): void {
  state.selectedAssetIndex = -1;
  state.selectedStoryPageIndex = 0;
  syncUrlState();
  render();
}

function stepViewer(delta: number): void {
  if (state.selectedAssetIndex < 0 || state.assets.length === 0) return;
  const nextIndex = Math.max(0, Math.min(state.assets.length - 1, state.selectedAssetIndex + delta));
  if (nextIndex !== state.selectedAssetIndex) {
    state.selectedAssetIndex = nextIndex;
    state.selectedStoryPageIndex = 0;
    syncUrlState();
    render();
  }
}

function stepStoryPage(delta: number): boolean {
  const pages = selectedStoryPages(state);
  if (pages.length === 0) return false;

  const nextPage = Math.max(0, Math.min(pages.length - 1, state.selectedStoryPageIndex + delta));
  if (nextPage === state.selectedStoryPageIndex) return false;

  state.selectedStoryPageIndex = nextPage;
  syncUrlState();
  render();
  return true;
}

function seekCurrentVideo(secondsDelta: number): boolean {
  const viewer = document.querySelector(".viewer video") as HTMLVideoElement | null;
  if (!viewer) return false;

  const max = Number.isFinite(viewer.duration) ? viewer.duration : Number.MAX_SAFE_INTEGER;
  viewer.currentTime = Math.max(0, Math.min(max, viewer.currentTime + secondsDelta));
  return true;
}

function handleGlobalKeydown(event: KeyboardEvent): void {
  if (selectedAsset(state) === null) return;

  const code = event.code;
  const closeMatch = state.keybinds.close.includes(code) || code === "Escape";
  const prevMatch = state.keybinds.previous.includes(code) || code === "ArrowLeft";
  const nextMatch = state.keybinds.next.includes(code) || code === "ArrowRight";

  if (closeMatch) {
    event.preventDefault();
    closeViewer();
    return;
  }

  if (prevMatch) {
    event.preventDefault();
    const asset = selectedAsset(state);
    if (asset?.pages && asset.pages.length > 0) {
      if (!stepStoryPage(-1)) stepViewer(-1);
      return;
    }

    const video = document.querySelector(".viewer video") as HTMLVideoElement | null;
    if (video && video.currentTime > state.videoSkipSeconds) {
      if (!seekCurrentVideo(-state.videoSkipSeconds)) stepViewer(-1);
    } else {
      stepViewer(-1);
    }
    return;
  }

  if (nextMatch) {
    event.preventDefault();
    const asset = selectedAsset(state);
    if (asset?.pages && asset.pages.length > 0) {
      if (!stepStoryPage(1)) stepViewer(1);
      return;
    }

    const video = document.querySelector(".viewer video") as HTMLVideoElement | null;
    if (video && Number.isFinite(video.duration)) {
      if (video.currentTime >= Math.max(0, video.duration - state.videoSkipSeconds)) {
        stepViewer(1);
      } else if (!seekCurrentVideo(state.videoSkipSeconds)) {
        stepViewer(1);
      }
      return;
    }

    stepViewer(1);
  }
}

function stopScanPolling(): void {
  if (scanPollTimer !== null) {
    window.clearInterval(scanPollTimer);
    scanPollTimer = null;
  }
}

function ensureScanPolling(): void {
  if (scanPollTimer !== null) return;

  scanPollTimer = window.setInterval(() => {
    void loadScanStatus();
  }, 1200);
}

function stopStartupPolling(): void {
  if (startupPollTimer !== null) {
    window.clearInterval(startupPollTimer);
    startupPollTimer = null;
  }
}

function updateStartupMessage(): void {
  const checks = state.startup.checks;

  if (!checks.bootstrap) {
    state.startup.message = "Connecting to backend...";
    return;
  }

  if (state.scan.status === "running") {
    state.startup.message = `Scanning media index: ${state.scan.percentage}% (${state.scan.processedArtists}/${state.scan.totalArtists})`;
    return;
  }

  if (!checks.profiles || !checks.appConfig || !checks.assets) {
    state.startup.message = "Loading gallery metadata...";
    return;
  }

  state.startup.message = "Preparing interface...";
}

function finalizeStartupIfReady(): void {
  const checks = state.startup.checks;
  if (checks.bootstrap && checks.profiles && checks.appConfig && checks.assets) {
    state.startup.ready = true;
    stopStartupPolling();
  }
}

async function runStartupTick(): Promise<void> {
  if (state.startup.ready) {
    stopStartupPolling();
    return;
  }

  state.startup.attempts += 1;
  logger.debug("[Startup] tick", {
    attempts: state.startup.attempts,
    checks: state.startup.checks
  });

  if (!state.startup.checks.bootstrap) {
    const bootstrapOk = await loadBootstrapStatus();
    if (bootstrapOk) {
      state.startup.checks.bootstrap = true;
      logger.debug("[Startup] backend reachable");
    } else {
      logger.debug("[Startup] backend not reachable yet");
    }

    updateStartupMessage();
    render();
    return;
  }

  await loadScanStatus();

  if (!state.startup.checks.profiles) {
    state.startup.checks.profiles = await loadProfiles();
    logger.debug("[Startup] profiles loaded", { ok: state.startup.checks.profiles });
  } else if (!state.startup.checks.appConfig) {
    state.startup.checks.appConfig = await loadAppConfig();
    logger.debug("[Startup] app config loaded", { ok: state.startup.checks.appConfig });
  } else if (!state.startup.checks.assets) {
    state.startup.checks.assets = await loadAssets();
    logger.debug("[Startup] assets loaded", { ok: state.startup.checks.assets });
  }

  updateStartupMessage();
  finalizeStartupIfReady();
  render();
}

function ensureStartupPolling(): void {
  if (state.startup.ready || startupPollTimer !== null) {
    return;
  }

  startupPollTimer = window.setInterval(() => {
    void runStartupTick();
  }, 1000);

  void runStartupTick();
}

async function loadBootstrapStatus(): Promise<boolean> {
  try {
    logger.debug("[Startup] requesting /v1/bootstrap-status");
    const res = await fetch("/v1/bootstrap-status");
    if (!res.ok) {
      logger.warn("[Startup] bootstrap-status not ready", { status: res.status });
      state.bootstrap.status = "running";
      state.bootstrap.message = `Waiting for backend (${res.status})`;
      return false;
    }

    const data = (await res.json()) as BootstrapStatus;
    state.bootstrap = data;
    logger.debug("[Startup] bootstrap-status ready", data);
    return true;
  } catch (error) {
    logger.warn("[Startup] bootstrap-status failed", error);
    state.bootstrap.status = "running";
    state.bootstrap.message = "Connecting to backend";
    return false;
  }
}

async function loadScanStatus(): Promise<boolean> {
  try {
    const res = await fetch("/v1/scan-status");
    if (!res.ok) return false;

    const data = (await res.json()) as ScanStatus;
    state.scan = data;
    state.isScanning = data.status === "running";

    if (state.isScanning) {
      ensureScanPolling();
    } else {
      stopScanPolling();
      if (data.status === "complete") {
        state.scanMessage = "Scan completed";
      }
    }

    render();
    return true;
  } catch {
    // Keep shell resilient during backend startup.
    return false;
  }
}

async function loadProfiles(): Promise<boolean> {
  try {
    const res = await fetch("/v1/profiles");
    if (!res.ok) return false;
    const data = (await res.json()) as ProfilesResponse;
    state.activeProfile = data.activeProfile;
    state.profiles = data.profiles;
    render();
    return true;
  } catch {
    // Keep shell resilient during backend startup.
    return false;
  }
}

async function switchProfile(name: string): Promise<void> {
  try {
    const res = await fetch("/v1/profiles/active", {
      method: "PUT",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ name })
    });

    if (!res.ok) {
      state.scanMessage = `Profile switch failed (${res.status})`;
      render();
      return;
    }

    state.scanMessage = `Switched to profile ${name}. Auto scan started.`;
    state.page = 1;
    state.selectedAssetIndex = -1;
    syncUrlState();
    await loadProfiles();
    await triggerScan();
  } catch {
    state.scanMessage = "Profile switch failed (network error)";
    render();
  }
}

async function triggerScan(): Promise<void> {
  try {
    const res = await fetch("/v1/scans", {
      method: "POST"
    });

    if (!res.ok) {
      state.scanMessage = `Scan trigger failed (${res.status})`;
      render();
      return;
    }

    const data = (await res.json()) as { runId: string };
    state.scanMessage = `Scan started. runId=${data.runId}`;
    state.isScanning = true;
    ensureScanPolling();
    render();

    await loadScanStatus();
    await loadAssets();
  } catch {
    state.scanMessage = "Scan trigger failed (network error)";
    render();
  }
}

async function loadAppConfig(): Promise<boolean> {
  try {
    const res = await fetch("/v1/app-config");
    if (!res.ok) return false;
    const data = (await res.json()) as AppConfigResponse;
    state.itemsPerPage = data.itemsPerPage;
    state.videoSkipSeconds = data.videoSkipSeconds;
    state.keybinds = data.keybinds;
    render();
    return true;
  } catch {
    // Keep shell resilient during backend startup.
    return false;
  }
}

async function loadAssets(): Promise<boolean> {
  try {
    const params = new URLSearchParams();
    if (state.searchText.trim()) params.set("text", state.searchText.trim());
    if (state.searchTags.trim()) params.set("q", state.searchTags.trim());
    params.set("page", String(state.page));
    params.set("limit", String(state.itemsPerPage));

    const res = await fetch(`/v1/assets?${params.toString()}`);
  if (!res.ok) return false;

    const data = (await res.json()) as AssetsResponse;
    state.assets = data.items;
    state.page = data.pagination.page;
    state.totalPages = data.pagination.totalPages;
    state.totalAssets = data.pagination.total;

    const viewId = new URLSearchParams(window.location.search).get("view");
    setSelectedAssetById(viewId);

    syncUrlState();
    render();
    return true;
  } catch {
    // Keep shell resilient during backend startup.
    return false;
  }
}

function view(model: UiState): VNode {
  if (!model.startup.ready) {
    const checkItems = [
      { label: "Backend reachable", ok: model.startup.checks.bootstrap },
      { label: "Profiles loaded", ok: model.startup.checks.profiles },
      { label: "App config loaded", ok: model.startup.checks.appConfig },
      { label: "Initial assets loaded", ok: model.startup.checks.assets }
    ];

    return h("main.shell", [
      h("section.startup", [
        h("h1.h1", "Media Gallery 2"),
        h("p.muted", model.startup.message),
        h("div.startupBar", [
          h("div.startupBarFill", {
            style: {
              width: `${Math.round((checkItems.filter((item) => item.ok).length / checkItems.length) * 100)}%`
            }
          })
        ]),
        h(
          "ul.startupChecks",
          checkItems.map((item) =>
            h(
              "li",
              {
                class: {
                  ok: item.ok
                }
              },
              `${item.ok ? "Ready" : "Waiting"} - ${item.label}`
            )
          )
        ),
        model.scan.status === "running"
          ? h(
              "p.muted",
              `Scan progress: ${model.scan.percentage}% (${model.scan.processedArtists}/${model.scan.totalArtists}) ${model.scan.currentArtist}`
            )
          : h("p.muted", `Startup attempts: ${model.startup.attempts}`)
      ])
    ]);
  }

  const profileOptions = model.profiles.map((profile) =>
    h(
      "option",
      {
        props: {
          value: profile.name,
          selected: profile.name === model.activeProfile
        }
      },
      profile.name
    )
  );

  const paginationControls = h("div.pager", [
    h(
      "button",
      {
        props: {
          disabled: model.page <= 1
        },
        on: {
          click: () => {
            if (model.page <= 1) return;
            state.page -= 1;
            syncUrlState();
            void loadAssets();
          }
        }
      },
      "Prev"
    ),
    h("span.pagerText", `Page ${model.page} / ${Math.max(model.totalPages, 1)} · ${model.totalAssets} assets`),
    h(
      "button",
      {
        props: {
          disabled: model.totalPages > 0 && model.page >= model.totalPages
        },
        on: {
          click: () => {
            if (model.totalPages > 0 && model.page >= model.totalPages) return;
            state.page += 1;
            syncUrlState();
            void loadAssets();
          }
        }
      },
      "Next"
    )
  ]);

  const mediaGrid = model.assets.length
    ? h(
        "section.grid",
        model.assets.map((asset, index) =>
          h(
            "article.asset",
            {
              on: {
                click: () => {
                  selectAssetAt(index);
                }
              }
            },
            [
              asset.pages && asset.pages.length > 0
                ? (() => {
                    const firstStoryPage = asset.pages[0] ?? asset.path;
                    return h("img.thumb", {
                      props: {
                        src: mediaUrl(firstStoryPage, true),
                        alt: `${asset.artist} / ${asset.name}`,
                        loading: "lazy"
                      }
                    });
                  })()
                : isVideoPath(asset.path)
                  ? h("img.thumb", {
                      props: {
                        src: mediaUrl(asset.path, true),
                        alt: `${asset.artist} / ${asset.name}`,
                        loading: "lazy"
                      }
                    })
                : h("img.thumb", {
                    props: {
                      src: mediaUrl(asset.path, true),
                      alt: `${asset.artist} / ${asset.name}`,
                      loading: "lazy"
                    }
                  }),
              h("div.assetMeta", [
                  h("div.assetTitleRow", [
                    h("strong", asset.name),
                    asset.pages && asset.pages.length > 0
                      ? h("span.storyCountBadge", String(asset.pages.length))
                      : null
                  ]),
                  h("span.assetArtist", asset.artist)
              ])
            ]
          )
        )
      )
    : h("div.empty", [
        h("h2", "No media found"),
        h("p", "Run scan, adjust profile roots, or change search filters.")
      ]);

  const current = selectedAsset(model);
  const currentStoryPages = selectedStoryPages(model);
  const currentStoryLabel = selectedStoryLabel(model);
  const viewer = current
    ? h(
        "section.viewer",
        {
          on: {
            click: (event: Event) => {
              if ((event.target as HTMLElement).classList.contains("viewer")) {
                closeViewer();
              }
            },
            wheel: (event: WheelEvent) => {
              const now = Date.now();
              if (now - lastWheelAt < 120) return;
              lastWheelAt = now;
              if (event.deltaY > 0) stepViewer(1);
              if (event.deltaY < 0) stepViewer(-1);
            }
          }
        },
        [
          h("div.viewerTop", [
            h("button", { on: { click: () => closeViewer() } }, "Close"),
            h("div.viewerTitle", `${current.artist} / ${current.name}${currentStoryLabel ? ` · ${currentStoryLabel}` : ""}`)
          ]),
          h("div.viewerBody", [
            currentStoryPages.length > 0
              ? h("img.viewerMedia", {
                  props: {
                    src: selectedMediaPath(model, false),
                    alt: `${current.artist} / ${current.name}`
                  }
                })
              : isVideoPath(current.path)
              ? h("video.viewerMedia", {
                  props: {
                    src: selectedMediaPath(model, false),
                    controls: true,
                    autoplay: true
                  }
                })
              : h("img.viewerMedia", {
                  props: {
                    src: selectedMediaPath(model, false),
                    alt: `${current.artist} / ${current.name}`
                  }
                })
          ])
        ]
      )
    : null;

  const progress = model.isScanning
    ? h("div.progress", [
        h("div.progressBar", { style: { width: `${Math.max(0, Math.min(100, model.scan.percentage))}%` } }),
        h(
          "p.muted",
          `Scanning ${model.scan.profile}: ${model.scan.percentage}% (${model.scan.processedArtists}/${model.scan.totalArtists}) ${model.scan.currentArtist}`
        )
      ])
    : null;

  return h("main.shell", [
    h("header.top", [
      h("div.title", [
        h("h1.h1", "Media Gallery 2"),
        h("p.muted", model.bootstrap.message)
      ]),
      h("div.actions", [
        h("span.badge", `profile:${model.activeProfile}`),
        h(
          "select",
          {
            props: { value: model.activeProfile },
            on: {
              change: (event: Event) => {
                const target = event.target as HTMLSelectElement;
                void switchProfile(target.value);
              }
            }
          },
          profileOptions
        ),
        h(
          "button",
          {
            props: { disabled: model.isScanning },
            on: {
              click: () => {
                void triggerScan();
              }
            }
          },
          model.isScanning ? "Scanning..." : "Run Scan"
        )
      ])
    ]),
    h(
      "form.toolbar",
      {
        on: {
          submit: (event: Event) => {
            event.preventDefault();
            state.page = 1;
            syncUrlState();
            void loadAssets();
          }
        }
      },
      [
      h("input.search", {
        props: {
          value: model.searchText,
          name: "text",
          placeholder: "Search title or artist"
        },
        on: {
          input: (event: Event) => {
            state.searchText = (event.target as HTMLInputElement).value;
          }
        }
      }),
      h("input.search", {
        props: {
          value: model.searchTags,
          name: "q",
          placeholder: "Tag query: tag1,tag2|-tag3"
        },
        on: {
          input: (event: Event) => {
            state.searchTags = (event.target as HTMLInputElement).value;
          }
        }
      }),
      h(
        "button",
        {
          on: {
            click: () => {
              state.page = 1;
              syncUrlState();
              void loadAssets();
            }
          }
        },
        "Search"
      )
    ]),
    progress ?? h("div"),
    model.scanMessage ? h("p.muted", model.scanMessage) : h("div"),
    paginationControls,
    mediaGrid,
    viewer ?? h("div")
  ]);
}

function render(): void {
  vnode = patch(vnode, view(state));
}

window.addEventListener("keydown", handleGlobalKeydown);
window.addEventListener("popstate", () => {
  applyUrlState();
  void loadAssets();
});

applyUrlState();
render();
ensureStartupPolling();
