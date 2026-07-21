document.addEventListener('DOMContentLoaded', () => {
    // 1. WebSocket Connection Setup
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
    
    const ws = new WebSocket(wsUrl);
    const logsContainer = document.getElementById('logs');
    const clearLogsBtn = document.getElementById('clear-logs-btn');

    if (clearLogsBtn) {
        clearLogsBtn.addEventListener('click', () => {
            if (logsContainer) logsContainer.innerHTML = '';
        });
    }

    ws.onmessage = (event) => {
        if (!logsContainer) return;

        const data = JSON.parse(event.data);
        const line = document.createElement('div');
        
        if (data.event === 'TASK_STARTED') {
            line.className = 'text-blue-400';
            line.textContent = `[${data.time}] ⚙️ Task Started: ${data.task.id} (${data.task.type})`;
        } else if (data.event === 'TASK_COMPLETED') {
            line.className = 'text-green-400';
            line.textContent = `[${data.time}] ✅ Task Succeeded: ${data.task.id}`;
        } else if (data.event === 'TASK_RETRY') {
            line.className = 'text-yellow-400';
            line.textContent = `[${data.time}] ⚠️ Task Retrying (${data.task.retries}/${data.task.max_retries}): ${data.task.id} - ${data.task.last_error}`;
        } else if (data.event === 'TASK_DLQ') {
            line.className = 'text-red-400 font-bold';
            line.textContent = `[${data.time}] 🚨 Task Moved to DLQ: ${data.task.id} - ${data.task.last_error}`;
        } else {
            line.textContent = `[${data.time}] ${JSON.stringify(data)}`;
        }

        logsContainer.appendChild(line);
        logsContainer.scrollTop = logsContainer.scrollHeight;
    };

    // 2. Safe HTMX Metric Card Updating
    document.body.addEventListener('htmx:afterOnLoad', (evt) => {
        if (evt.detail.xhr && evt.detail.xhr.responseURL.includes('/api/stats')) {
            try {
                const stats = JSON.parse(evt.detail.xhr.responseText);
                
                const elPending = document.getElementById('stat-pending');
                const elProcessing = document.getElementById('stat-processing');
                const elDelayed = document.getElementById('stat-delayed');
                const elDlq = document.getElementById('stat-dlq');

                if (elPending) elPending.innerText = stats.pending ?? 0;
                if (elProcessing) elProcessing.innerText = stats.processing ?? 0;
                if (elDelayed) elDelayed.innerText = stats.delayed ?? 0;
                if (elDlq) elDlq.innerText = stats.dlq ?? 0;
            } catch (err) {
                console.error("Error parsing stats JSON:", err);
            }
        }
    });

    // 3. DLQ Auto-Poll Function
    async function fetchDLQ() {
        try {
            const res = await fetch('/api/dlq');
            const tasks = await res.json();
            const tbody = document.getElementById('dlq-rows');
            if (!tbody) return;

            if (!tasks || tasks.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="p-3 text-center text-gray-500">No failed tasks in DLQ currently</td></tr>';
                return;
            }
            
            tbody.innerHTML = tasks.map(t => `
                <tr class="border-b border-gray-800 hover:bg-gray-850">
                    <td class="p-2 font-mono text-indigo-300">${t.id}</td>
                    <td class="p-2">${t.type}</td>
                    <td class="p-2 text-red-400 font-semibold">${t.retries}/${t.max_retries}</td>
                    <td class="p-2 text-red-300 font-mono text-[10px]">${t.last_error}</td>
                    <td class="p-2 font-mono text-gray-400 text-[10px]">${t.payload}</td>
                </tr>
            `).join('');
        } catch (err) {}
    }

    setInterval(fetchDLQ, 3000);
});