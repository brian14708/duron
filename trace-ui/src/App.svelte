<svelte:options customElement={{ tag: "my-element" }} />

<script lang="ts">
  import "./app.css";
  let { name = "world" } = $props();
</script>

<h1 class="text-3xl font-bold underline">Hello {name}!</h1>
<slot />

<style>
  h1 {
    color: purple;
  }
</style>
