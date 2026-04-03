// TossIt Dashboard JavaScript

let refreshInterval;
const REFRESH_RATE = 5000;

document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 TossIt Dashboard loaded');
    loadAllData();
    startAutoRefresh();
    loadSettings();
});

// Tab Management
function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
    });
    document.getElementById(tabName).classList.add('active');
    event.target.classList.add('active');
}

// Auto-refresh
function startAutoRefresh() {
    refreshInterval = setInterval(() => {
        loadAllData();
    }, REFRESH_RATE);
}

function stopAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
}

async function loadAllData() {
    await Promise.all([
        loadClusterStats(),
        loadNodes(),
        loadFiles(),
        loadJobsStatus()
    ]);
}

// Cluster Stats
async function loadClusterStats() {
    try {
        const timestamp = new Date().getTime();
        const [statsResponse, settingsResponse] = await Promise.all([
            fetch(`/api/cluster/stats?_=${timestamp}`),
            fetch(`/api/settings?_=${timestamp}`)
        ]);

        const stats = await statsResponse.json();
        const settings = await settingsResponse.json();

        const totalCapacityGB = stats.total_capacity_gb || 0;
        const uniqueDataGB = stats.unique_data_gb || 0;
        const actualStorageUsedGB = stats.actual_storage_used_gb || 0;
        const freeCapacityGB = stats.free_capacity_gb || 0;
        const replicationFactor = stats.replication_factor || 1;

        document.getElementById('totalNodes').textContent = stats.online_nodes || 0;
        document.getElementById('usableCapacity').textContent = totalCapacityGB.toFixed(1);
        document.getElementById('rawCapacity').textContent = `${(stats.raw_capacity_gb || totalCapacityGB).toFixed(1)} GB raw`;
        document.getElementById('usedCapacity').textContent = uniqueDataGB.toFixed(1) + ' GB';
        document.getElementById('totalFiles').textContent = stats.total_files || 0;

        const totalChunksEl = document.getElementById('totalChunks');
        if (totalChunksEl) totalChunksEl.textContent = stats.total_chunks || 0;

        const replicationFactorEl = document.getElementById('replicationFactor');
        if (replicationFactorEl) replicationFactorEl.textContent = `${replicationFactor}x`;

        const systemHealthEl = document.getElementById('systemHealth');
        if (systemHealthEl) {
            const healthPercent = totalCapacityGB > 0
                ? Math.round((1 - (uniqueDataGB / totalCapacityGB)) * 100)
                : 100;
            systemHealthEl.textContent = `${healthPercent}%`;
        }

        const capacityBar = document.getElementById('capacityUsed');
        if (capacityBar) {
            const usedPercent = totalCapacityGB > 0 ? (uniqueDataGB / totalCapacityGB * 100) : 0;
            capacityBar.style.width = `${usedPercent}%`;
            const percentSpan = document.getElementById('capacityPercentage');
            if (percentSpan) percentSpan.textContent = `${usedPercent.toFixed(1)}%`;
        }

        const usedGBEl = document.getElementById('usedGB');
        const freeGBEl = document.getElementById('freeGB');
        const efficiencyEl = document.getElementById('storageEfficiency');

        if (usedGBEl) {
            usedGBEl.textContent = `${uniqueDataGB.toFixed(1)} GB`;
            usedGBEl.title = `Unique data: ${uniqueDataGB.toFixed(1)} GB\nPhysical storage: ${actualStorageUsedGB.toFixed(1)} GB (${replicationFactor}x replication)`;
        }
        if (freeGBEl) freeGBEl.textContent = `${freeCapacityGB.toFixed(1)} GB`;
        if (efficiencyEl) {
            efficiencyEl.textContent = `${(stats.storage_efficiency_percent || 0).toFixed(1)}%`;
            efficiencyEl.title = `Usable space after accounting for ${replicationFactor}x replication`;
        }

        const currentReplicasEl = document.getElementById('currentReplicas');
        if (currentReplicasEl) {
            const avgReplicas = stats.avg_replicas_per_chunk || 0;
            currentReplicasEl.textContent = `${avgReplicas.toFixed(1)} avg (target: ${replicationFactor}x)`;
            currentReplicasEl.style.color = stats.replication_complete
                ? '#22c55e'
                : (avgReplicas >= 1 ? '#f59e0b' : '#ef4444');
        }

    } catch (error) {
        console.error('Error loading cluster stats:', error);
    }
}

// Load Nodes
async function loadNodes() {
    try {
        const timestamp = new Date().getTime();
        const response = await fetch(`/api/nodes?_=${timestamp}`);
        const nodes = await response.json();

        const tbody = document.getElementById('nodesTable');
        tbody.innerHTML = '';

        nodes.forEach(node => {
            const usedGB = (node.total_capacity_gb || 0) - (node.free_capacity_gb || 0);
            const totalGB = node.total_capacity_gb || 1;
            const utilization = ((usedGB / totalGB) * 100).toFixed(1);

            const row = `
                <tr>
                    <td>${node.name || 'Unknown'}</td>
                    <td><span class="status-badge status-${node.status}">${node.status}</span></td>
                    <td>${totalGB.toFixed(1)} GB</td>
                    <td>${(node.free_capacity_gb || 0).toFixed(1)} GB</td>
                    <td>
                        <div style="display: flex; align-items: center; gap: 0.5rem;">
                            <div style="flex: 1; background: #e2e8f0; height: 20px; border-radius: 10px; overflow: hidden;">
                                <div style="width: ${utilization}%; height: 100%; background: linear-gradient(90deg, #22c55e, #2563eb);"></div>
                            </div>
                            <span>${utilization}%</span>
                        </div>
                    </td>
                    <td>${(node.priority_score || 0).toFixed(2)}</td>
                    <td>${node.last_heartbeat_seconds_ago !== undefined ?
                        (node.last_heartbeat_seconds_ago === 0 ? 'Just now' : `${node.last_heartbeat_seconds_ago}s ago`) :
                        'Unknown'}</td>
                    <td><button class="btn btn-secondary" onclick="viewNodeDetails(${node.id})">Details</button></td>
                </tr>
            `;
            tbody.innerHTML += row;
        });
    } catch (error) {
        console.error('Error loading nodes:', error);
    }
}

// Load Files
async function loadFiles() {
    try {
        const timestamp = new Date().getTime();
        const response = await fetch(`/api/files?_=${timestamp}`);
        const files = await response.json();

        const tbody = document.getElementById('filesTable');
        tbody.innerHTML = '';

        if (files.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No files uploaded yet</td></tr>';
            return;
        }

        files.forEach(file => {
            const sizeMB = file.size_mb || file.total_size_mb || (file.total_size_bytes / (1024 * 1024));
            const displaySize = typeof sizeMB === 'number' ? sizeMB.toFixed(2) : '0.00';

            // ── FIX: UUID v7 IDs contain hyphens — must be quoted in onclick ──
            // Without quotes, JS interprets the hyphens as minus operators and
            // throws a syntax error, silently breaking delete/download/details.
            const fileIdStr = String(file.id);

            const row = `
                <tr>
                    <td>${file.filename}</td>
                    <td>${displaySize} MB</td>
                    <td>${file.total_chunks || 1}</td>
                    <td><span class="status-badge status-complete">Complete</span></td>
                    <td>${file.created_at ? new Date(file.created_at).toLocaleString() : 'Unknown'}</td>
                    <td>
                        <button class="btn btn-secondary" onclick="viewFileDetails('${fileIdStr}')">📋 Details</button>
                        <button class="btn btn-primary"   onclick="downloadFile('${fileIdStr}')">⬇️</button>
                        <button class="btn btn-danger"    onclick="deleteFile('${fileIdStr}')">🗑️</button>
                    </td>
                </tr>
            `;
            tbody.innerHTML += row;
        });
    } catch (error) {
        console.error('Error loading files:', error);
    }
}

// Load Jobs Status
async function loadJobsStatus() {
    try {
        const timestamp = new Date().getTime();
        const response = await fetch(`/api/jobs/status?_=${timestamp}`);
        const jobs = await response.json();

        document.getElementById('pendingJobs').textContent = jobs.pending || 0;
        document.getElementById('inProgressJobs').textContent = jobs.in_progress || 0;
        document.getElementById('completedJobs').textContent = jobs.completed || 0;
        document.getElementById('failedJobs').textContent = jobs.failed || 0;

        const activeJobsEl = document.getElementById('activeJobs');
        if (activeJobsEl) {
            activeJobsEl.textContent = (jobs.pending || 0) + (jobs.in_progress || 0);
        }
    } catch (error) {
        console.error('Error loading jobs:', error);
    }
}

// Load Settings
async function loadSettings() {
    try {
        const response = await fetch('/api/settings');
        const settings = await response.json();

        document.getElementById('chunk_size_mb').value = settings.chunk_size_mb || 64;
        document.getElementById('min_replicas').value = settings.min_replicas || 2;
        document.getElementById('max_replicas').value = settings.max_replicas || 3;
        document.getElementById('replication_strategy').value = settings.replication_strategy || 'priority';
        document.getElementById('redundancy_mode').value = settings.redundancy_mode || 'standard';
        document.getElementById('verify_on_upload').checked = settings.verify_on_upload !== false;
        document.getElementById('parallel_downloads').checked = settings.parallel_downloads !== false;
    } catch (error) {
        console.error('Error loading settings:', error);
    }
}

// File Search/Filter
function filterFiles() {
    const searchTerm = document.getElementById('fileSearch').value.toLowerCase();
    const tbody = document.getElementById('filesTable');
    const rows = tbody.getElementsByTagName('tr');
    for (let row of rows) {
        const filename = row.cells[0]?.textContent.toLowerCase() || '';
        row.style.display = filename.includes(searchTerm) ? '' : 'none';
    }
}

// Upload File
async function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];

    if (!file) {
        alert('Please select a file');
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    const progressContainer = document.getElementById('uploadProgress');
    const progressFill = document.getElementById('progressBar');
    const statusText = document.getElementById('uploadStatus');

    progressContainer.style.display = 'block';
    if (progressFill) {
        progressFill.style.width = '50%';
        progressFill.textContent = 'Uploading...';
    }
    if (statusText) statusText.textContent = `Uploading ${file.name}...`;

    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            const result = await response.json();
            if (progressFill) {
                progressFill.style.width = '100%';
                progressFill.textContent = '100%';
            }
            if (statusText) statusText.textContent = 'Upload complete!';
            setTimeout(() => {
                progressContainer.style.display = 'none';
                fileInput.value = '';
                loadFiles();
                loadClusterStats();
            }, 2000);
        } else {
            const error = await response.json();
            if (statusText) statusText.textContent = `Upload failed: ${error.detail || 'Unknown error'}`;
        }
    } catch (error) {
        console.error('Upload error:', error);
        if (statusText) statusText.textContent = `Upload error: ${error.message}`;
    }
}

// Download File — fileId is always a quoted string (UUID v7)
function downloadFile(fileId) {
    window.location.href = `/api/download/${fileId}`;
}

// Delete File — fileId is always a quoted string (UUID v7)
async function deleteFile(fileId) {
    if (!confirm('Are you sure you want to delete this file?')) {
        return;
    }

    try {
        const response = await fetch(`/api/files/${fileId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            alert('File deleted successfully');
            loadFiles();
            loadClusterStats();
        } else {
            const data = await response.json().catch(() => ({}));
            alert(`Failed to delete file: ${data.detail || response.status}`);
        }
    } catch (error) {
        console.error('Error deleting file:', error);
        alert('Error deleting file');
    }
}

// View File Details — fileId is always a quoted string (UUID v7)
async function viewFileDetails(fileId) {
    try {
        const response = await fetch(`/api/files/${fileId}`);
        if (!response.ok) {
            alert('Could not load file details');
            return;
        }

        const file = await response.json();
        const sizeMB = (file.total_size_bytes / (1024 * 1024)).toFixed(2);

        const modalContent = `
            <h2>📋 File Details</h2>
            <div class="db-status">
                <p><strong>Filename:</strong> ${file.filename}</p>
                <p><strong>Size:</strong> ${sizeMB} MB (${file.total_size_bytes.toLocaleString()} bytes)</p>
                <p><strong>Chunks:</strong> ${file.total_chunks || 1}</p>
                <p><strong>Checksum (SHA256):</strong> <code style="font-size: 0.8em; word-break: break-all;">${file.checksum_sha256}</code></p>
                <p><strong>Uploaded:</strong> ${file.created_at ? new Date(file.created_at).toLocaleString() : 'Unknown'}</p>
                <p><strong>Uploaded By:</strong> ${file.uploaded_by || 'Unknown'}</p>
                <p><strong>Status:</strong> ${file.is_complete ? '✅ Complete' : '⏳ Incomplete'}</p>
                <p><strong>Replicas:</strong> ${file.replica_count || 1} node(s)</p>
            </div>
        `;

        showModal(modalContent);
    } catch (error) {
        console.error('Error loading file details:', error);
        alert('Error loading file details: ' + error.message);
    }
}

// View Node Details
function viewNodeDetails(nodeId) {
    alert('Node details coming in next update');
}

// Modal
function showModal(content) {
    const modal = document.getElementById('fileDetailsModal');
    const modalContent = document.getElementById('modalContent');
    modalContent.innerHTML = content;
    modal.style.display = 'block';
}

function closeModal() {
    document.getElementById('fileDetailsModal').style.display = 'none';
}

window.onclick = function(event) {
    const modal = document.getElementById('fileDetailsModal');
    if (event.target == modal) modal.style.display = 'none';
};

// Save Settings
async function saveSettings() {
    const settings = {
        chunk_size_mb: parseInt(document.getElementById('chunk_size_mb').value),
        min_replicas: parseInt(document.getElementById('min_replicas').value),
        max_replicas: parseInt(document.getElementById('max_replicas').value),
        replication_strategy: document.getElementById('replication_strategy').value,
        redundancy_mode: document.getElementById('redundancy_mode').value,
        verify_on_upload: document.getElementById('verify_on_upload').checked,
        parallel_downloads: document.getElementById('parallel_downloads').checked
    };

    try {
        const response = await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(settings)
        });

        const statusDiv = document.getElementById('settingsStatus');
        if (response.ok) {
            statusDiv.innerHTML = '<div class="status-message success">Settings saved successfully!</div>';
        } else {
            statusDiv.innerHTML = '<div class="status-message error">Failed to save settings</div>';
        }
        setTimeout(() => { statusDiv.innerHTML = ''; }, 3000);
    } catch (error) {
        document.getElementById('settingsStatus').innerHTML =
            '<div class="status-message error">Settings saved locally (backend not implemented)</div>';
        setTimeout(() => { document.getElementById('settingsStatus').innerHTML = ''; }, 3000);
    }
}

// Health Check
async function runHealthCheck() {
    const indicator = document.getElementById('healthIndicator');
    indicator.innerHTML = '<div class="health-icon">⏳</div><div class="health-text">Running validation...</div>';

    const resultsDiv = document.getElementById('validationResults');
    resultsDiv.innerHTML = '<p>Running validation...</p>';

    try {
        const response = await fetch('/api/acid/validate');
        if (!response.ok) throw new Error('Validation endpoint not available');

        const result = await response.json();

        if (result.is_consistent) {
            indicator.innerHTML = '<div class="health-icon">✅</div><div class="health-text">System is healthy</div>';
        } else {
            indicator.innerHTML = '<div class="health-icon">⚠️</div><div class="health-text">Issues detected</div>';
        }

        let html = '<h3>Validation Results</h3>';
        html += `<p><strong>Status:</strong> ${result.is_consistent ? '✅ Consistent' : '❌ Issues Found'}</p>`;

        if (result.violations?.length > 0) {
            html += '<h4>Violations:</h4>';
            result.violations.forEach(v => { html += `<div class="validation-item error">${v}</div>`; });
        }
        if (result.warnings?.length > 0) {
            html += '<h4>Warnings:</h4>';
            result.warnings.forEach(w => { html += `<div class="validation-item warning">${w}</div>`; });
        }
        if (result.is_consistent) {
            html += '<div class="validation-item success">✅ All checks passed!</div>';
        }
        resultsDiv.innerHTML = html;
    } catch (error) {
        indicator.innerHTML = '<div class="health-icon">❓</div><div class="health-text">Validation not available</div>';
        resultsDiv.innerHTML = '<p class="text-muted">Health validation endpoint not yet implemented.</p>';
    }
}

// Repair Tools
async function recalculateCapacity() {
    const statusDiv = document.getElementById('repairStatus');
    statusDiv.innerHTML = '<div class="status-message">Recalculating capacity...</div>';
    try {
        await loadClusterStats();
        await loadNodes();
        statusDiv.innerHTML = '<div class="status-message success">✅ Capacity recalculated</div>';
    } catch (error) {
        statusDiv.innerHTML = '<div class="status-message error">❌ Failed to recalculate</div>';
    }
    setTimeout(() => { statusDiv.innerHTML = ''; }, 3000);
}

async function fixIncompleteFiles() {
    document.getElementById('repairStatus').innerHTML = '<div class="status-message">⚠️ Feature not yet implemented</div>';
    setTimeout(() => { document.getElementById('repairStatus').innerHTML = ''; }, 3000);
}

async function cleanupOrphans() {
    if (!confirm('This will remove database records with no corresponding files. Continue?')) return;
    document.getElementById('repairStatus').innerHTML = '<div class="status-message">⚠️ Feature not yet implemented</div>';
    setTimeout(() => { document.getElementById('repairStatus').innerHTML = ''; }, 3000);
}

async function cleanupStaleFiles() {
    if (!confirm('This will delete incomplete files older than 24 hours. Continue?')) return;
    document.getElementById('repairStatus').innerHTML = '<div class="status-message">⚠️ Feature not yet implemented</div>';
    setTimeout(() => { document.getElementById('repairStatus').innerHTML = ''; }, 3000);
}

async function cleanupJobs() {
    if (!confirm('This will remove completed and failed jobs older than 7 days. Continue?')) return;
    alert('Job cleanup feature not yet implemented');
}
