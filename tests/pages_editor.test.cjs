"use strict";

// Execute the shipped page module without a browser or third-party packages.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const html = fs.readFileSync(path.join(__dirname, "../pages/workflow-editor/index.html"), "utf8");
const moduleSource = html.match(/<script type="module">([\s\S]*?)<\/script>/)[1]
  .replace(/\ninit\(\);\s*$/, "");

function editor() {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {
        value: "", textContent: "", innerHTML: "", checked: false, disabled: false,
        style: {}, dataset: {}, options: [],
        classList: {
          add: (...values) => values.forEach(value => classes.add(value)),
          toggle: (value, enabled) => enabled ? classes.add(value) : classes.delete(value),
          contains: value => classes.has(value),
        },
        addEventListener() {}, setAttribute() {}, appendChild() {}, remove() {},
        focus() {}, close() {}, showModal() {}, querySelectorAll: () => [],
      });
    }
    return elements.get(id);
  }
  const bridge = {};
  const context = vm.createContext({
    window: { AstrBotPluginPage: bridge, addEventListener() {} },
    document: {
      getElementById: element, querySelectorAll: () => [],
      addEventListener() {}, createElement: () => element(Symbol()),
    },
    structuredClone, console, setTimeout() {}, setInterval() {},
  });
  vm.runInContext(moduleSource, context);
  const run = code => vm.runInContext(code, context);
  run(`state.workflows = [{name: "fixture", workflow_id: "42", region: "overseas",
    instance_type: "Standard", description: "saved", llm_enabled: true,
    llm_enhance: false, llm_template_path: "", output_type: "auto",
    capability: "multi_reference", cost_hint: "fixture cost", prompt_profile: "edit",
    nodes: [{node_id: "1", field_name: "prompt", value_type: "prompt", field_value: ""}]}];
    state.original = clone(state.workflows); state.loading = false; setDirty(true);`);
  return { bridge, element, run };
}

test("saving an unchanged snapshot marks only that snapshot as saved", async () => {
  const page = editor();
  let submitted;
  page.bridge.apiPost = async (route, payload) => {
    assert.equal(route, "page/config");
    submitted = structuredClone(payload.workflows);
    return { success: true, message: "saved" };
  };
  await page.run("saveConfig()");
  assert.equal(page.run("state.dirty"), false);
  assert.equal(page.run("state.saving"), false);
  assert.equal(page.element("saveBtn").disabled, true);
  assert.deepEqual(JSON.parse(page.run("JSON.stringify(state.original)")), submitted);
});

test("edits made while saving remain unsaved after the response", async () => {
  const page = editor();
  let respond, submitted;
  page.bridge.apiPost = (_route, payload) => {
    submitted = structuredClone(payload.workflows);
    return new Promise(resolve => { respond = resolve; });
  };
  const pending = page.run("saveConfig()");
  page.run('mutate(() => { state.workflows[0].description = "edited while saving"; });');
  assert.equal(page.element("saveBtn").textContent, "保存中…");
  respond({ success: true });
  await pending;
  assert.equal(submitted[0].description, "saved");
  assert.equal(page.run("state.original[0].description"), "saved");
  assert.equal(page.run("state.workflows[0].description"), "edited while saving");
  assert.equal(page.run("state.dirty"), true);
  assert.equal(page.element("saveBtn").disabled, false);
  assert.equal(page.element("dirtyBadge").classList.contains("show"), true);
});

test("failed saves preserve the last saved snapshot and pending edits", async () => {
  const page = editor();
  page.run('mutate(() => { state.workflows[0].description = "pending edit"; });');
  page.bridge.apiPost = async () => { throw new Error("offline"); };
  await page.run("saveConfig()");
  assert.equal(page.run("state.original[0].description"), "saved");
  assert.equal(page.run("state.workflows[0].description"), "pending edit");
  assert.equal(page.run("state.dirty"), true);
  assert.equal(page.run("state.saving"), false);
  assert.equal(page.element("saveBtn").disabled, false);
});

test("editing workflow presentation preserves upstream metadata and native identity", () => {
  const page = editor();
  page.run("openWorkflowDialog(0)");
  page.element("wfName").value = "renamed";
  page.element("wfDescription").value = "changed purpose";
  page.run("saveWorkflowFromDialog()");
  const workflow = JSON.parse(page.run("JSON.stringify(state.workflows[0])"));
  assert.equal(workflow.name, "renamed");
  assert.equal(workflow.description, "changed purpose");
  assert.equal(workflow.workflow_id, "42");
  assert.equal(workflow.capability, "multi_reference");
  assert.equal(workflow.cost_hint, "fixture cost");
  assert.equal(workflow.prompt_profile, "edit");
  assert.equal(workflow.nodes.length, 1);
});

test("editing a node preserves media aliases, required flags and parameter bounds", () => {
  const page = editor();
  page.run(`state.workflows[0].nodes = [{node_id: "3", field_name: "width", value_type: "text",
    field_value: "512", label: "width", input_key: "resolution", role: "output width",
    required: true, param_type: "integer", minimum: 256, maximum: 2048, choices: ["512", "1024"]}];
    openNodeDialog(0, 0);`);
  page.element("nodeWorkflow").value = "fixture";
  page.element("nodeValue").value = "1024";
  page.run("saveNodeFromDialog()");
  const node = JSON.parse(page.run("JSON.stringify(state.workflows[0].nodes[0])"));
  assert.equal(node.field_value, "1024");
  assert.equal(node.input_key, "resolution");
  assert.equal(node.role, "output width");
  assert.equal(node.required, true);
  assert.equal(node.minimum, 256);
  assert.equal(node.maximum, 2048);
  assert.deepEqual(node.choices, ["512", "1024"]);
});

test("page validation allows short choice labels supported by backend normalization", () => {
  const page = editor();
  for (const value of ["16:9", "16：9", "16:9 (widescreen)"]) {
    const result = page.run(`validateNode({node_id: "424", field_name: "aspect_ratio", value_type: "text",
      field_value: ${JSON.stringify(value)}, choices: ["16:9 (Widescreen)", "1:1 (Square)"]})`);
    assert.equal(result.ok, true, value);
  }
});
