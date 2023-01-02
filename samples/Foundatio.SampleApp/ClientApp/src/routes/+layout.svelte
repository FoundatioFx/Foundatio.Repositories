<script>
	import '../app.scss';
	import NavMenu from './NavMenu.svelte';

	import { onMount } from "svelte";
  onMount(async () => {
		await import('bootstrap/js/src/collapse.js');
  });
</script>

<div class="page">
	<div class="sidebar">
			<NavMenu />
	</div>

	<main>
			<article class="content px-4">
				<slot />
			</article>
	</main>
</div>
