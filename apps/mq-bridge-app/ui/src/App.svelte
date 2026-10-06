<script lang="ts">
  import ConsumersPanel from "./components/ConsumersPanel.svelte";
  import Dialog from "./components/Dialog.svelte";
  import MainTabs from "./components/MainTabs.svelte";
  import PublishersPanel from "./components/PublishersPanel.svelte";
  import SettingsPanel from "./components/SettingsPanel.svelte";
  import StatusPanel from "./components/StatusPanel.svelte";
  import { setBasePath } from "@awesome.me/webawesome/dist/utilities/base-path.js";

  // Point to the location where webawesome assets are served.
  // For most Vite/Tauri setups, "/" works as it refers to the public root.
  setBasePath("/");
</script>

<MainTabs />
<Dialog />

<div class="app-body">
  <ConsumersPanel />
  <PublishersPanel />
  <SettingsPanel />
  <StatusPanel />
</div>
