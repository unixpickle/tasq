package main

const Homepage = `<!doctype html>
<html>
	<head>
		<meta charset="utf-8">
		<style type="text/css">
			html, body {
				background-color: #f0f0f0;
				text-align: center;
				font-family: sans-serif;
			}

			@media screen and (max-width: 620px) {
				.width-sizing {
					display: block;
					margin: 0 10px;
					width: calc(100% - 20px);
				}
			}

			@media screen and (min-width: 620px) {
				.width-sizing {
					display: block;
					margin: 0 auto;
					width: 600px;
				}
			}

			.panel {
				display: block;
				position: relative;
				box-sizing: border-box;
				background-color: white;
				border: 1px solid #d5d5d5;
				padding: 10px;
				margin-bottom: 10px;
			}

			.hidden {
				display: none;
			}

			#counts-list {
				list-style-type: none;
				padding: 0;
			}

			#counts-list > li {
				list-style: none;
			}

			#counts-list.counts-loading {
				pointer-events: none;
			}

			#counts-list.counts-loading li {
				display: none;
			}

			#counts-list.counts-loading::before {
				display: block;
				text-align: center;
				content: "Loading...";
			}

			.counts-item {
				width: 100%;
				margin: 10px 0;
			}

			.counts-item-name, .stats-name {
				display: block;
				border-bottom: 1px solid #d5d5d5;
				font-weight: bolder;
				margin-bottom: 10px;
				padding-bottom: 2px;
			}

			.counts-item.collapsed .counts-item-name {
				border: none;
				margin-bottom: 0;
			}

			.counts-item.collapsed .counts-item-table,
			.counts-item.collapsed .counts-item-actions {
				display: none;
			}

			.counts-item-name-default {
				font-style: oblique;
			}

			.counts-item-collapser {
				position: absolute;
				left: 5px;
				top: 5px;
				padding: 5px 10px;
				margin: 0;
				border: none;
				background: transparent;
				font-size: 1em;
				color: #555;
				cursor: pointer;
				font-family: monospace;
			}

			.counts-item-collapser::after {
				content: '▼';
			}

			.counts-item.collapsed > .counts-item-collapser::after {
				content: '▶';
			}

			.counts-item-table, .stats-table {
				text-align: left;
				margin: auto;
			}

			.counts-item-table td.counts-item-field-name, .stats-table td.stats-field-name {
				text-align: right;
				padding-right: 0.2em;
			}

			.counts-item-table td, .stats-table td {
				padding-bottom: 0.1em;
				padding-top: 0.1em;
			}

			button:focus {
				outline: 0;
			}

			.counts-item-action, .overlay-close-button {
				position: relative;
				margin: 5px;
				padding: 5px 10px;
				border: none;
				font-size: 1.2em;
				color: white;
				background-color: #999;
				cursor: pointer;
			}

			.counts-item-action:hover, .overlay-close-button:hover {
				background-color: #7b7b7b;
			}

			.counts-item-action-destructive {
				background-color: #ee6666;
			}

			.counts-item-action-destructive:hover {
				background-color: #cc5555;
			}

			#error-box {
				text-align: center;
				color: red;
			}

			#add-task-box > h1 {
				margin: 0 0 20px 0;
				padding: 0;
				font-size: 1.2em;
			}

			.add-task-field {
				margin: 8px 0;
			}

			.add-task-field label {
				text-align: right;
				margin: 0 2px 0 0;
				width: calc(30%);
			}

			.add-task-field input {
				width: calc(60%);
			}

			.add-task-field label, .add-task-field input {
				display: inline-block;
			}

			.overlay-container {
				display: block;
				position: fixed;
				width: 100%;
				height: 100%;
				top: 0;
				left: 0;
				background-color: rgba(0, 0, 0, 0.5);
			}

			.overlay-container-hidden {
				display: none;
			}

			.overlay-pane {
				text-align: center;
				position: absolute;
				background-color: white;
				top: 50px;
				height: 50%;
			}

			@media screen and (min-width: 620px) {
				.overlay-pane {
					width: 580px;
					left: calc(50% - 290px);
				}
			}

			@media screen and (max-width: 620px) {
				.overlay-pane {
					width: calc(100% - 40px);
					left: 20px;
				}
			}

			.overlay-textbox {
				display: block;
				height: calc(100% - 72px);
				width: calc(100% - 20px);
				resize: none;
				margin: 10px;
				border: 1px solid #d5d5d5;
				box-sizing: border-box;
			}
		</style>
	</head>
	<body>
		<ol id="counts-list" class="width-sizing counts-loading"></ol>
		<div id="empty-box" class="width-sizing panel hidden">
			There are no active queues.
		</div>
		<div id="error-box" class="width-sizing panel hidden"></div>
		<form id="add-task-box" class="width-sizing panel" onsubmit="return quickAddTask(event);">
			<h1>Quickly add a task</h1>
			<div class="add-task-field">
				<label>Context:</label>
				<input id="add-task-context" placeholder="(Leave empty for default context)">
			</div>
			<div class="add-task-field">
				<label>Task contents:</label>
				<input id="add-task-contents">
			</div>
			<input id="add-task-button" type="submit" value="Add task">
		</form>
		<li id="stats-box" class="width-sizing panel">
			<label class="stats-name">System stats</label>
			<table class="stats-table">
				<tr>
					<td class="stats-field-name">Uptime:</td>
					<td id="stats-field-uptime">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">Allocated:</td>
					<td id="stats-field-allocated">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">Total allocated:</td>
					<td id="stats-field-total-allocated">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">System allocated:</td>
					<td id="stats-field-sys-allocated">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">Last GC:</td>
					<td id="stats-field-last-gc">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">Last save:</td>
					<td id="stats-field-save-elapsed">-</td>
				</tr>
				<tr>
					<td class="stats-field-name">Save latency:</td>
					<td id="stats-field-save-latency">-</td>
				</tr>
			</table>
		</li>
		<div id="text-overlay-container" class="overlay-container overlay-container-hidden" onclick="closeTextOverlay()">
			<div class="overlay-pane" onclick="event.stopPropagation()">
				<textarea class="overlay-textbox"></textarea>
				<button class="overlay-close-button" onclick="closeTextOverlay()">Close</button>
			</div>
		</div>

		<script type="text/javascript">
		<!--
		const countsList = document.getElementById('counts-list');
		const emptyBox = document.getElementById('empty-box');
		const errorBox = document.getElementById('error-box');

		function queueNamePrefix() {
			const urlParams = new URLSearchParams(window.location.search);
			return urlParams.get('prefix') || '';
		}

		async function reloadCounts(actionFn) {
			countsList.classList.add('counts-loading');
			emptyBox.classList.add('hidden');
			errorBox.classList.add('hidden');
			let result;
			try {
				if (actionFn) {
					await actionFn();
				}
				result = await (await fetch('/counts?all=1&window=60')).json();
			} catch (e) {
				errorBox.textContent = '' + e;
				errorBox.classList.remove('hidden');
				return false;
			} finally {
				countsList.innerHTML = '';
				countsList.classList.remove('counts-loading');
			}

			const prefix = queueNamePrefix();

			const counts = result['data']['counts'];
			let numDisplayed = 0;

			const collapsed = JSON.parse(localStorage['collapsed'] || '[]');
			const allNames = [];
			counts.forEach((counts, i) => {
				const name = result['data']['names'][i];
				allNames.push(name);
				if (name.startsWith(prefix)) {
					addCountsToList(name, counts, collapsed.includes(name));
					numDisplayed++;
				}
			});
			// Don't endlessly cache collapsed data about deleted queues.
			localStorage['collapsed'] = JSON.stringify(
				collapsed.filter((x) => allNames.includes(x)),
			);

			if (numDisplayed === 0) {
				emptyBox.classList.remove('hidden');
			}

			await reloadStats();

			return true;
		}

		async function reloadStats() {
			const response = await (await fetch('/stats')).json();
			const stats = response['data'];
			[
				['stats-field-uptime', Math.round(stats.uptime) + ' seconds'],
				['stats-field-allocated', stats.memory.alloc + ' bytes'],
				['stats-field-total-allocated', stats.memory.totalAlloc + ' bytes'],
				['stats-field-sys-allocated', stats.memory.sys + ' bytes'],
				['stats-field-last-gc', stats.memory.lastGC.toFixed(2) + ' seconds ago'],
				['stats-field-save-elapsed', stats.save.elapsed.toFixed(2) + ' seconds ago'],
				['stats-field-save-latency', stats.save.latency.toFixed(3) + ' seconds'],
			].forEach((pair) => {
				const [fieldID, value] = pair;
				document.getElementById(fieldID).textContent = value;
			});
		}

		function addCountsToList(name, counts, collapsed) {
			const elem = document.createElement('li');
			elem.className = 'counts-item panel';
			if (collapsed) {
				elem.classList.add('collapsed');
			}

			const collapser = document.createElement('button');
			collapser.className = 'counts-item-collapser';
			collapser.addEventListener('click', () => toggleCollapse(elem, name));
			elem.appendChild(collapser);

			const nameLabel = document.createElement('label');
			nameLabel.className = 'counts-item-name';
			nameLabel.textContent = name || 'Default context';
			if (!name) {
				nameLabel.classList.add('counts-item-name-default');
			}
			elem.appendChild(nameLabel);

			const fields = [
				['pending', 'Pending'],
				['running', 'In progress'],
				['expired', 'Expired'],
				['completed', 'Completed'],
				['rate', 'Tasks/sec'],
				['modtime', 'Last modified'],
			];
			const fieldTable = document.createElement('table');
			fieldTable.className = 'counts-item-table';
			const tableBody = document.createElement('tbody');
			fields.forEach((field) => {
				const [fieldId, caption] = field;
				const row = document.createElement('tr');
				const labelCol = document.createElement('td');
				labelCol.className = 'counts-item-field-name';
				labelCol.textContent = caption + ':';
				const dataCol = document.createElement('td');
				if (fieldId === 'rate') {
					dataCol.textContent = counts[fieldId].toFixed(3);
				} else if (fieldId == 'modtime') {
					dataCol.textContent = relativeTimeSince(counts[fieldId]);
				} else {
					dataCol.textContent = '' + counts[fieldId];
				}
				row.appendChild(labelCol);
				row.appendChild(dataCol);
				tableBody.appendChild(row);
			});
			fieldTable.appendChild(tableBody);
			elem.appendChild(fieldTable);

			const actions = document.createElement('div');
			actions.className = 'counts-item-actions';

			[
				['Peek', peekTask],
				['Push', pushTaskPrompt],
				['Expire All', expireAll],
				['Delete', deleteContext],
			].forEach((item) => {
				const [actionName, actionFn] = item;
				const actionButton = document.createElement('button');
				actionButton.className = 'counts-item-action';
				if (actionName === 'Expire All' || actionName === 'Delete') {
					actionButton.classList.add('counts-item-action-destructive');
				}
				actionButton.textContent = actionName;
				actionButton.addEventListener('click', () => actionFn(name));
				actions.appendChild(actionButton);
			});

			elem.appendChild(actions);

			countsList.appendChild(elem);
		}

		function relativeTimeSince(timestamp) {
			const now = Date.now();
			const since = Math.max(0, now - timestamp) / 1000;
			if (since < 60) {
				const seconds = Math.round(since);
				if (seconds == 1) {
					return seconds + ' second ago';
				} else {
					return seconds + ' seconds ago';
				}
			} else if (since < 60*60) {
				const minutes = Math.round(since / 60);
				if (minutes == 1) {
					return minutes + ' minute ago';
				} else {
					return minutes + ' minutes ago';
				}
			} else if (since < 60*60*24) {
				const hours = Math.round(since / 60 / 60);
				if (hours == 1) {
					return hours + ' hour ago';
				} else {
					return hours + ' hours ago';
				}
			} else {
				const days = Math.round(since / 60 / 60 / 24);
				if (days == 1) {
					return days + ' day ago';
				} else {
					return days + ' days ago';
				}
			}
		}

		function toggleCollapse(elem, name) {
			const collapsed = JSON.parse(localStorage['collapsed'] || '[]');
			const idx = collapsed.indexOf(name);
			if (idx < 0) {
				collapsed.push(name);
				elem.classList.add('collapsed');
			} else {
				collapsed.splice(idx, 1);
				elem.classList.remove('collapsed');
			}
			localStorage['collapsed'] = JSON.stringify(collapsed);
		}

		function deleteContext(name) {
			if (confirm('Really delete queue with name: "' + name + '"?')) {
				reloadCounts(() => fetch('/task/clear?context=' + encodeURIComponent(name)));				
			}
		}

		function expireAll(name) {
			reloadCounts(() => fetch('/task/expire_all?context=' + encodeURIComponent(name)));
		}

		async function peekTask(name) {
			try {
				const response = await fetch('/task/peek?context=' + encodeURIComponent(name));
				showTextOverlay(JSON.stringify(await response.json(), null, 2));
			} catch (e) {
				alert(e);
			}
		}

		async function pushTaskPrompt(name) {
			const contents = prompt('Enter task contents');
			if (!contents) {
				return;
			}
			try {
				let value = null;
				await reloadCounts(async () => {
					const pushURL = '/task/push?context=' + encodeURIComponent(name) +
						'&contents=' + encodeURIComponent(contents);
					const resp = await fetch(pushURL)
					value = await resp.text();
				});
			} catch (e) {
				alert(e);
			}
		}

		function quickAddTask(e) {
			e.preventDefault();
			const context = document.getElementById('add-task-context').value;
			const contentsField = document.getElementById('add-task-contents');
			const contents = contentsField.value;
			reloadCounts(() => {
				return fetch('/task/push?context=' + encodeURIComponent(context) + '&contents=' +
					encodeURIComponent(contents));
			}).then((success) => {
				if (success) {
					contentsField.value = '';
				}
			});
			return false;
		}

		function showTextOverlay(text) {
			const container = document.getElementById('text-overlay-container');
			container.getElementsByClassName('overlay-textbox')[0].value = text;
			container.classList.remove('overlay-container-hidden');
		}

		function closeTextOverlay() {
			const container = document.getElementById('text-overlay-container');
			container.classList.add('overlay-container-hidden');
		}

		reloadCounts(null);
		-->
		</script>
	</body>
</html>
`
