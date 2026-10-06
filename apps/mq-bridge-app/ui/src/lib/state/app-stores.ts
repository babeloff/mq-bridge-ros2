import { writable } from "svelte/store";
import type { MainTab, RuntimeStatus } from "../runtime-status";
import { EMPTY_RUNTIME_STATUS } from "../runtime-status";
import type { ConsumerTreeNode } from "../consumer-grouping";
import type { PublisherTreeNode } from "../publisher-grouping";
import { EMPTY_STORAGE_SECURITY, type StorageSecurityInfo } from "../storage-security";

function initialMainTab(): MainTab {
  if (typeof window === "undefined") return "publishers";
  const hash = window.location.hash || "";
  if (hash === "#config") return "config";
  if (hash === "#status") return "status";
  if (hash === "#consumers" || hash.startsWith("#consumers:")) return "consumers";
  return "publishers";
}

export const activeMainTab = writable<MainTab>(initialMainTab());
export const runtimeStatusStore = writable<RuntimeStatus>(EMPTY_RUNTIME_STATUS);
export const storageSecurityStore = writable<StorageSecurityInfo>({ ...EMPTY_STORAGE_SECURITY });
export const workspaceDirtyStore = writable(false);
export const workspaceSavingStore = writable(false);

// Shared by the publishers and consumers panels so the dividers keep their position across tab
// switches. `detailPaneTopPercent` is the height share of the pane above the horizontal divider.
export const sidebarWidthStore = writable(280);
export const detailPaneTopPercent = writable(40);
export const MIN_DETAIL_PANE_PERCENT = 20;
export const MAX_DETAIL_PANE_PERCENT = 80;

export interface ConsumerSidebarItem {
  name: string;
  displayName: string;
  inputProto: string;
  statusClass: string;
  messageCount: number;
  throughputLabel: string;
  originalIndex: number;
  id?: string;
}

export interface ConsumerLogItem {
  timeLabel: string;
  payloadPreview: string;
  messageIndex: number;
  selected: boolean;
}

export interface ConsumersPanelState {
  hasConsumers: boolean;
  currentConsumerKey: string | null;
  items: ConsumerSidebarItem[];
  groupedItems: ConsumerTreeNode[];
  selectedIndex: number;
  selectedMessageIndex: number;
  activeSubtab: "definition" | "response" | "messages";
  isNew: boolean;
  deleteLabel: string;
  messageCaptureEnabled: boolean;
  messageCaptureKeepLast: number;
  outputMode: "none" | "publisher" | "response";
  publisherOptions: Array<{ value: string; label: string }>;
  selectedPublisher: string;
  responseSupported: boolean;
  responseHeaders: Array<{ id: number; key: string; value: string; enabled: boolean }>;
  responsePayload: string;
  liveStatusText: string;
  liveStatusVariant: "success" | "danger" | "neutral";
  toggleLabel: string;
  toggleVariant: "success" | "danger" | "neutral";
  toggleBusy: boolean;
  messages: ConsumerLogItem[];
  detailInfo: string;
  detailRequestPayload: string;
  detailRequestHeaders: Array<[string, string]>;
  detailResponsePayload: string;
  detailResponseHeaders: Array<[string, string]>;
  detailRequestContentType: string;
  detailResponseContentType: string;
  hasResponse: boolean;
}

export const consumersPanelState = writable<ConsumersPanelState>({
  hasConsumers: false,
  currentConsumerKey: null,
  items: [],
  groupedItems: [],
  selectedIndex: 0,
  selectedMessageIndex: -1,
  activeSubtab: "messages",
  isNew: false,
  deleteLabel: "Delete",
  messageCaptureEnabled: true,
  messageCaptureKeepLast: 100,
  outputMode: "none",
  publisherOptions: [],
  selectedPublisher: "",
  responseSupported: false,
  responseHeaders: [],
  responsePayload: "",
  liveStatusText: "Consumer Stopped",
  liveStatusVariant: "neutral",
  toggleLabel: "Start",
  toggleVariant: "success",
  toggleBusy: false,
  messages: [],
  detailInfo: "Select a message to view details",
  detailRequestPayload: "",
  detailRequestHeaders: [],
  detailResponsePayload: "",
  detailResponseHeaders: [],
  detailRequestContentType: "",
  detailResponseContentType: "",
  hasResponse: false,
});

export interface PublisherHistoryRow {
  historyIndex: number;
  timeLabel: string;
  statusLabel: string;
  statusClass: string;
  payloadPreview: string;
  pinned: boolean;
}

export interface PublisherSidebarItem {
  name: string;
  endpointType: string;
  originalIndex: number;
}

export interface PublisherRequestFieldState {
  label: string;
  placeholder: string;
  value: string;
  visible: boolean;
  /** Desktop only: offer a native file picker beside the input. */
  browse?: boolean;
}

export interface PublisherHeaderRow {
  id: number;
  key: string;
  value: string;
  enabled: boolean;
}

export interface PublishersPanelState {
  hasPublishers: boolean;
  items: PublisherSidebarItem[];
  groupedItems: PublisherTreeNode[];
  selectedIndex: number;
  selectedHistoryIndex: number;
  activeSubtab: "payload" | "headers" | "history" | "definition";
  isNew: boolean;
  deleteLabel: string;
  endpointType: string;
  methodVisible: boolean;
  methodValue: string;
  extraFieldOne: PublisherRequestFieldState;
  extraFieldTwo: PublisherRequestFieldState;
  urlField: PublisherRequestFieldState;
  requestPayload: string;
  metadataRows: PublisherHeaderRow[];
  responseVisible: boolean;
  responseTabLabel: string;
  responseStatusLabel: string;
  responseStatusText: string;
  responseStatusColor: string;
  responseDurationLabel: string;
  responseSizeLabel: string;
  requestRows: Array<[string, string]>;
  requestHeaders: Array<[string, string]>;
  responseHeaders: Array<[string, string]>;
  responsePayload: string;
  historyRows: PublisherHistoryRow[];
}

export const publishersPanelState = writable<PublishersPanelState>({
  hasPublishers: false,
  items: [],
  groupedItems: [],
  selectedIndex: 0,
  selectedHistoryIndex: -1,
  activeSubtab: "payload",
  isNew: false,
  deleteLabel: "Delete",
  endpointType: "",
  methodVisible: false,
  methodValue: "POST",
  extraFieldOne: { label: "Target", placeholder: "", value: "", visible: false },
  extraFieldTwo: { label: "Target", placeholder: "", value: "", visible: false },
  urlField: { label: "URL", placeholder: "", value: "", visible: true },
  requestPayload: "",
  metadataRows: [],
  responseVisible: true,
  responseTabLabel: "Response",
  responseStatusLabel: "Ready",
  responseStatusText: "",
  responseStatusColor: "var(--text-primary)",
  responseDurationLabel: "",
  responseSizeLabel: "",
  requestRows: [],
  requestHeaders: [],
  responseHeaders: [],
  responsePayload: "",
  historyRows: [],
});
