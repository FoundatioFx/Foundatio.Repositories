<script>
	import { onMount } from 'svelte';
	import { search } from '$lib/stores/stores.js';

	let data = {
		reviews: [],
		categoryCounts: [],
		tagCounts: []
	};

	let categories = [];
	let tags = [];
	let searchValue = '';

	search.subscribe(async (value) => {
		searchValue = value;
		await getReviews();
	});

	async function setCategory(category) {
		if (category.length == 0) categories = [];
		else if (!categories.includes(category)) categories = [...categories, category];

		await getReviews();
	}

	async function setTag(tag) {
		if (tag.length == 0) tags = [];
		else if (!tags.includes(tag)) tags = [...tags, tag];

		await getReviews();
	}

	async function getReviews() {
		var filter = '';
		if (categories.length > 0 && tags.length > 0)
			filter =
				'(' +
				categories.map((c) => 'category:"' + c + '"').join(' AND ') +
				') AND (' +
				tags.map((t) => 'tags:"' + t + '"').join(' AND ') +
				')';
		else if (categories.length > 0)
			filter = categories.map((c) => 'category:"' + c + '"').join(' AND ');
		else if (tags.length > 0) filter = tags.map((t) => 'tags:"' + t + '"').join(' AND ');

		const res = await fetch(`/api/gamereviews?search=${searchValue}&filter=${filter}`);
		data = await res.json();
	}

	onMount(async () => {
		await getReviews();
	});
</script>

<h5>Categories{categories.length > 0 ? ' (' + categories.join(', ') + ')' : ''}</h5>
{#each data.categoryCounts as category}
	<button
		type="button"
		class="btn btn-outline-secondary btn-sm"
		on:click={() => setCategory(category.name)}
	>
		{category.name} <span class="badge text-bg-primary">{category.total}</span>
	</button>

	<span>&nbsp;</span>
{/each}

{#if categories.length > 0}
	<button type="button" class="btn btn-secondary btn-sm" on:click={() => setCategory('')}>
		Clear Filter
	</button>
{/if}

<h5>Tags{tags.length > 0 ? ' (' + tags.join(', ') + ')' : ''}</h5>
{#each data.tagCounts as tag}
	<button type="button" class="btn btn-outline-secondary btn-sm" on:click={() => setTag(tag.name)}>
		{tag.name} <span class="badge text-bg-primary">{tag.total}</span>
	</button>

	<span>&nbsp;</span>
{/each}

{#if tags.length > 0}
	<button type="button" class="btn btn-secondary btn-sm" on:click={() => setTag('')}>
		Clear Filter
	</button>
{/if}

<table class="table pt-20">
	<thead>
		<tr>
			<th>Date</th>
			<th>Name</th>
			<th>Category</th>
			<th>Tags</th>
		</tr>
	</thead>
	<tbody>
		{#each data.reviews as review}
			<tr>
				<td>{review.createdUtc}</td>
				<td>{review.name}</td>
				<td>{review.category}</td>
				<td>{review.tags.join(', ')}</td>
			</tr>
		{/each}
	</tbody>
</table>
