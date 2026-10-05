// Renders ```mermaid code blocks. Without JavaScript the diagram source stays readable.
(async () => {
  const blocks = document.querySelectorAll("code.language-mermaid");
  if (!blocks.length) return;
  const { default: mermaid } = await import("https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs");
  for (const code of blocks) {
    const div = document.createElement("div");
    div.className = "mermaid";
    div.textContent = code.textContent;
    code.parentElement.replaceWith(div);
  }
  const dark = ["navy", "coal", "ayu"].some((t) => document.documentElement.classList.contains(t));
  mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default" });
  await mermaid.run();
})();
