console.log('MQTT Data Monitor loaded successfully!');
console.log('Theme: HiveMQ & Azure Event Grid inspired');

// Function to update stat counters
function updateStatsCards(stats) {
    console.log('Updating stats cards with:', stats);
    
    // Update Duplicated Messages
    const duplicatedElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-error');
    if (duplicatedElement) {
        duplicatedElement.textContent = stats.critical || 0;
    }
    
    // Update QOS=2 Messages
    const qos2Element = document.querySelector('.text-2xl.font-semibold.text-mqtt-warning');
    if (qos2Element) {
        qos2Element.textContent = stats.in_progress || 0;
    }
    
    // Update Unique Topics
    const topicsElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-success');
    if (topicsElement) {
        topicsElement.textContent = stats.resolved_today || 0;
    }
    
    // Update Total Messages
    const totalElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-azure');
    if (totalElement) {
        totalElement.textContent = stats.avg_response_time || 0;
    }
}

// Function to update table with new data
function updateTableWithData(data) {
    const tableBody = document.getElementById('messageTableBody');
    
    // Clear existing rows (except the "no results" row)
    const noResultsRow = document.getElementById('noResultsRow');
    tableBody.innerHTML = '';
    if (noResultsRow) {
        tableBody.appendChild(noResultsRow);
    }
    
    // If no data, show empty state
    if (!data || data.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="5" class="px-6 py-12 text-center">
                    <div class="text-mqtt-text-light">
                        <i class="fas fa-database text-4xl mb-4 text-mqtt-azure"></i>
                        <p class="text-lg font-medium text-mqtt-text mb-2">No Data Found</p>
                        <p class="text-sm">No messages found in the specified table.</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }
    
    // Build HTML for each row
    data.forEach(row => {
        const isDup = String(row.is_duplicate || 'false').toLowerCase();
        const dupColor = isDup === 'true' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800';
        const dupText = isDup === 'true' ? 'Yes' : 'No';
        
        const qosValue = String(row.qos || 0);
        const qosColor = qosValue === '2' ? 'bg-blue-100 text-blue-800' : 
                       qosValue === '1' ? 'bg-yellow-100 text-yellow-800' : 
                       'bg-gray-100 text-gray-800';
        
        const message = String(row.message || '');
        const messageDisplay = message.length > 100 ? message.substring(0, 100) + '...' : message;
        
        const topic = String(row.topic || 'N/A');
        const receivedTime = String(row.received_time || 'N/A');
        
        const rowHtml = `
            <tr class="mqtt-hover" data-topic="${topic.toLowerCase()}" data-qos="${qosValue}" data-duplicate="${isDup}">
                <td class="px-6 py-4">
                    <div class="text-sm text-mqtt-text max-w-md">
                        <div class="truncate" title="${message}">${messageDisplay}</div>
                    </div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full ${dupColor}">
                        ${dupText}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="inline-flex px-2 py-1 text-xs font-semibold rounded-full ${qosColor}">
                        QoS ${qosValue}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text">
                    <span class="font-mono text-xs bg-mqtt-bg px-2 py-1 rounded">${topic}</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-mqtt-text-light">
                    ${receivedTime}
                </td>
            </tr>
        `;
        
        tableBody.insertAdjacentHTML('beforeend', rowHtml);
    });
    
    // Re-apply search filter if active
    const searchInput = document.getElementById('searchInput');
    if (searchInput && searchInput.value) {
        filterTable();
    }
}

// Notification Functions
function showNotification(type, title, message) {
    const modal = document.getElementById('notificationModal');
    const icon = document.getElementById('notificationIcon');
    const titleEl = document.getElementById('notificationTitle');
    const messageEl = document.getElementById('notificationMessage');
    const button = document.getElementById('notificationButton');
    
    // Set content
    titleEl.textContent = title;
    messageEl.textContent = message;
    
    // Set icon and colors based on type
    if (type === 'success') {
        icon.innerHTML = '<div class="w-12 h-12 bg-green-100 rounded-full flex items-center justify-center"><i class="fas fa-check-circle text-2xl text-mqtt-success"></i></div>';
        titleEl.className = 'text-lg font-medium text-mqtt-success';
        button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-success hover:bg-green-700';
    } else if (type === 'error') {
        icon.innerHTML = '<div class="w-12 h-12 bg-red-100 rounded-full flex items-center justify-center"><i class="fas fa-times-circle text-2xl text-mqtt-error"></i></div>';
        titleEl.className = 'text-lg font-medium text-mqtt-error';
        button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-error hover:bg-red-700';
    } else if (type === 'info') {
        icon.innerHTML = '<div class="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center"><i class="fas fa-info-circle text-2xl text-mqtt-azure"></i></div>';
        titleEl.className = 'text-lg font-medium text-mqtt-azure';
        button.className = 'w-full px-4 py-2 rounded-lg text-white font-medium bg-mqtt-azure hover:bg-blue-700';
    }
    // Show modal
    modal.classList.remove('hidden');
    
    // Auto-close after 5 seconds
    setTimeout(() => {
        closeNotification();
    }, 5000);
}

function closeNotification() {
    const modal = document.getElementById('notificationModal');
    modal.classList.add('hidden');
}

// Filter table function for search
function filterTable() {
    const searchInput = document.getElementById('searchInput');
    const searchTerm = searchInput.value.toLowerCase().trim();
    const tableBody = document.getElementById('messageTableBody');
    const rows = tableBody.getElementsByTagName('tr');
    const rowsArray = Array.from(rows).filter(row => row.id !== 'noResultsRow');
    
    rowsArray.forEach(function(row) {
        if (searchTerm === '') {
            // Show all rows when search is empty
            row.style.display = '';
        } else {
            // Get searchable data from row attributes (only topic/source)
            const topic = row.getAttribute('data-topic') || '';
            
            // Search only in the topic/source field
            const searchableText = topic;
            
            // Show/hide row based on search match
            if (searchableText.includes(searchTerm)) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        }
    });
    
    // Update search results count and show/hide no results message
    const visibleRows = rowsArray.filter(row => row.style.display !== 'none').length;
    const noResultsRow = document.getElementById('noResultsRow');
    
    if (searchTerm !== '' && visibleRows === 0) {
        noResultsRow.style.display = '';
    } else {
        noResultsRow.style.display = 'none';
    }
}

// Tab switching functionality
function switchTab(activeTab, activeContent, inactiveTab, inactiveContent) {
    // Update tab appearance
    activeTab.classList.add('active', 'border-mqtt-azure', 'text-mqtt-azure');
    activeTab.classList.remove('border-transparent', 'text-mqtt-text-light');
    
    inactiveTab.classList.remove('active', 'border-mqtt-azure', 'text-mqtt-azure');
    inactiveTab.classList.add('border-transparent', 'text-mqtt-text-light');
    
    // Show/hide content
    activeContent.classList.remove('hidden');
    inactiveContent.classList.add('hidden');
}

// DOMContentLoaded event handler
document.addEventListener('DOMContentLoaded', function() {
    const connectionTab = document.getElementById('connectionTab');
    const dashboardTab = document.getElementById('dashboardTab');
    const connectionContent = document.getElementById('connectionContent');
    const dashboardContent = document.getElementById('dashboardContent');
    
    // Connection tab click handler
    connectionTab.addEventListener('click', function() {
        switchTab(connectionTab, connectionContent, dashboardTab, dashboardContent);
    });
    
    // Dashboard tab click handler
    dashboardTab.addEventListener('click', function() {
        switchTab(dashboardTab, dashboardContent, connectionTab, connectionContent);
    });
    
    // MQTT Configuration Form Handlers
    const mqttForm = document.getElementById('mqttConfigForm');
    const testConnectionBtn = document.getElementById('testConnection');
    const saveConfigBtn = document.getElementById('saveConfig');
    const connectAndMonitorBtn = document.getElementById('connectAndMonitor');
    
    console.log('✅ Form elements found:', 'mqttForm:', !!mqttForm, 'testConnectionBtn:', !!testConnectionBtn, 'saveConfigBtn:', !!saveConfigBtn, 'connectAndMonitorBtn:', !!connectAndMonitorBtn);
    
    if (!testConnectionBtn) {
        console.error('❌ Test Connection button not found!');
        return;
    }
    console.log('🔧 Attaching event listener to Test Connection button...');
    
    // Test Connection Handler
    testConnectionBtn.addEventListener('click', async function(e) {
        console.log('🔌 Test Connection button clicked!');
        e.preventDefault();
        e.stopPropagation();
        
        const brokerAddress = document.getElementById('broker_address').value;
        console.log('Broker address:', brokerAddress);
        
        if (!brokerAddress) {
            showNotification('error', 'Missing Broker Address', 'Please enter a broker address first.');
            return;
        }
        // Show loading state
        const originalHTML = this.innerHTML;
        this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Testing Connection...';
        this.disabled = true;
        
        try {
            // Get form data
            const formData = new FormData(mqttForm);
            const config = {};
            // Convert FormData to object
            for (const [key, value] of formData.entries()) {
                config[key] = value;
            }
            // Handle checkboxes
            config['require_tls'] = document.getElementById('require_tls').checked ? 'true' : 'false';
            config['tls_disable_certs'] = document.getElementById('tls_disable_certs').checked ? 'true' : 'false';
            
            console.log('🔍 Testing MQTT connection with config:', config);
            console.log('📤 Sending request to /api/test-mqtt-connection');
            
            // Send test request to backend API
            const response = await fetch('/api/test-mqtt-connection', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(config)
            });
            
            console.log('📥 Response status:', response.status);
            const result = await response.json();
            console.log('📊 Response data:', result);
            
            if (result.success) {
                // Success
                this.innerHTML = '<i class="fas fa-check mr-2"></i>Connection Successful!';
                this.classList.remove('bg-mqtt-secondary');
                this.classList.add('bg-mqtt-success');
                
                // Show success modal
                showNotification('success', 'Connection Successful!', result.message);
                
                // Reset button after delay
                setTimeout(() => {
                    this.innerHTML = originalHTML;
                    this.classList.remove('bg-mqtt-success');
                    this.classList.add('bg-mqtt-secondary');
                    this.disabled = false;
                }, 3000);
            } else {
                // Failure
                throw new Error(result.error || result.message || 'Connection failed');
            }
        } catch (error) {
            console.error('MQTT connection test failed:', error);
            
            // Show error state
            this.innerHTML = '<i class="fas fa-times mr-2"></i>Connection Failed';
            this.classList.remove('bg-mqtt-secondary');
            this.classList.add('bg-mqtt-error');
            
            // Show error modal
            showNotification('error', 'Connection Failed', error.message);
            
            // Reset button after delay
            setTimeout(() => {
                this.innerHTML = originalHTML;
                this.classList.remove('bg-mqtt-error');
                this.classList.add('bg-mqtt-secondary');
                this.disabled = false;
            }, 3000);
        }
    });
    
    console.log('✅ Test Connection event listener attached successfully!');
    
    // Save Configuration Handler
    saveConfigBtn.addEventListener('click', function() {
        const formData = new FormData(mqttForm);
        const config = Object.fromEntries(formData);
        
        // Save to localStorage (in real app, this would be saved to backend)
        localStorage.setItem('mqttConfig', JSON.stringify(config));
        
        this.innerHTML = '<i class="fas fa-check mr-2"></i>Saved!';
        this.classList.remove('bg-mqtt-warning');
        this.classList.add('bg-mqtt-success');
        
        setTimeout(() => {
            this.innerHTML = '<i class="fas fa-save mr-2"></i>Save Configuration';
            this.classList.remove('bg-mqtt-success');
            this.classList.add('bg-mqtt-warning');
        }, 2000);
    });
    
    // Connect and Monitor Handler
    connectAndMonitorBtn.addEventListener('click', async function(e) {
        e.preventDefault();
        
        const brokerAddress = document.getElementById('broker_address').value;
        if (!brokerAddress) {
            alert('Please enter a broker address first.');
            return;
        }
        // Show loading state
        const originalHTML = this.innerHTML;
        this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Starting MQTT Job...';
        this.disabled = true;
        
        try {
            // Get form data
            const formData = new FormData(mqttForm);
            const config = {};
            // Convert FormData to object, handling checkboxes properly
            for (const [key, value] of formData.entries()) {
                config[key] = value;
            }
            // Handle checkboxes (they won't be in formData if unchecked)
            config['require_tls'] = document.getElementById('require_tls').checked ? 'true' : 'false';
            config['tls_disable_certs'] = document.getElementById('tls_disable_certs').checked ? 'true' : 'false';
            
            // Save configuration locally
            localStorage.setItem('mqttConfig', JSON.stringify(config));
            
            console.log('📤 Sending MQTT Configuration to server:', config);
            console.log('📊 Data will be written to:', `${config.catalog}.${config.schema}.${config.table}`);
            
            // Send configuration to backend API
            const response = await fetch('/api/start-mqtt-job', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(config)
            });
            
            const result = await response.json();
            
            if (result.success) {
                // Success - show confirmation and switch to dashboard
                this.innerHTML = '<i class="fas fa-check mr-2"></i>Job Started!';
                this.classList.remove('bg-mqtt-azure');
                this.classList.add('bg-mqtt-success');
                
                showNotification('success', 'Job Started Successfully!', result.message || `Job ID: ${result.job_id}, Run ID: ${result.run_id}`);
                
                // Switch to dashboard tab after a short delay
                setTimeout(() => {
                    switchTab(dashboardTab, dashboardContent, connectionTab, connectionContent);
                    this.innerHTML = originalHTML;
                    this.classList.remove('bg-mqtt-success');
                    this.classList.add('bg-mqtt-azure');
                    this.disabled = false;
                }, 2000);
            } else {
                // Error from server
                throw new Error(result.error || 'Unknown error occurred');
            }
        } catch (error) {
            console.error('Error starting MQTT job:', error);
            alert(`Failed to start MQTT job: ${error.message}`);
            
            // Reset button state
            this.innerHTML = originalHTML;
            this.disabled = false;
        }
    });
    
    // Refresh Dashboard Data Handler
    const refreshDataBtn = document.getElementById('refreshDataBtn');
    console.log('Refresh button element:', refreshDataBtn);
    if (refreshDataBtn) {
        refreshDataBtn.addEventListener('click', async function() {
            console.log('Refresh button clicked!');
            const catalog = document.getElementById('catalog').value;
            const schema = document.getElementById('schema').value;
            const table = document.getElementById('table').value;
            
            console.log('Form values:', { catalog, schema, table });
            
            if (!catalog || !schema || !table) {
                showNotification('error', 'Missing Information', 'Please enter catalog, schema, and table name.');
                return;
            }
            
            // Show loading state
            const originalHTML = this.innerHTML;
            this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Refreshing...';
            this.disabled = true;
            
            try {
                console.log('Sending refresh request...');
                const response = await fetch('/api/refresh-data', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        catalog: catalog,
                        schema: schema,
                        table: table,
                    })
                });
                
                console.log('Response status:', response.status);
                const result = await response.json();
                console.log('Response data:', result);
                
                if (result.success) {
                    showNotification('success', 'Data Refreshed!', `Loaded ${result.row_count} rows from ${catalog}.${schema}.${table}`);
                    
                    // Update the table with new data
                    console.log('Updating table with data:', result.data);
                    updateTableWithData(result.data);
                    
                    // Update the stats cards
                    if (result.stats) {
                        console.log('Updating stats:', result.stats);
                        updateStatsCards(result.stats);
                    }
                    
                    // Reset button state
                    this.innerHTML = originalHTML;
                    this.disabled = false;
                } else {
                    throw new Error(result.error || 'Failed to refresh data');
                }
            } catch (error) {
                console.error('Error refreshing data:', error);
                showNotification('error', 'Refresh Failed', error.message);
                
                // Reset button state
                this.innerHTML = originalHTML;
                this.disabled = false;
            }
        });
    }
    
    // Stealth Auto-Refresh Function (runs every 30 seconds in the background)
    async function stealthRefresh() {
        const catalog = document.getElementById('catalog').value;
        const schema = document.getElementById('schema').value;
        const table = document.getElementById('table').value;
        
        // Only refresh if all fields are filled
        if (!catalog || !schema || !table) {
            return;
        }
        
        try {
            console.log('Auto-refreshing data in background...');
            const response = await fetch('/api/refresh-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    catalog: catalog,
                    schema: schema,
                    table: table,
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                console.log(`Auto-refresh successful: ${result.row_count} rows loaded`);
                // Update the table silently without notification
                updateTableWithData(result.data);
                // Update the stats cards
                if (result.stats) {
                    updateStatsCards(result.stats);
                }
            } else {
                console.error('Auto-refresh failed:', result.error);
            }
        } catch (error) {
            console.error('Auto-refresh error:', error);
        }
    }
    
    // Start auto-refresh interval (every 30 seconds)
    setInterval(stealthRefresh, 30000);
    console.log('Auto-refresh enabled: Data will refresh every 30 seconds');
    
    // TLS Settings Toggle Handler
    const requireTlsCheckbox = document.getElementById('require_tls');
    const tlsSettings = document.getElementById('tlsSettings');
    const tlsDisableCertsCheckbox = document.getElementById('tls_disable_certs');
    const certificateFields = document.querySelectorAll('#ca_certs, #certfile, #keyfile');
    
    // Function to toggle TLS settings visibility
    function toggleTlsSettings() {
        const portField = document.getElementById('port');
        
        if (requireTlsCheckbox.checked) {
            tlsSettings.classList.remove('hidden');
            // Auto-update port to TLS default if it's still at non-TLS default
            if (portField.value === '1883') {
                portField.value = '8883';
            }
        } else {
            tlsSettings.classList.add('hidden');
            // Auto-update port to non-TLS default if it's still at TLS default
            if (portField.value === '8883') {
                portField.value = '1883';
            }
        }
    }
    // Function to toggle certificate fields based on disable verification checkbox
    function toggleCertificateFields() {
        certificateFields.forEach(field => {
            if (tlsDisableCertsCheckbox.checked) {
                field.disabled = true;
                field.classList.add('bg-gray-100', 'text-gray-400');
                field.value = '';
            } else {
                field.disabled = false;
                field.classList.remove('bg-gray-100', 'text-gray-400');
            }
        });
    }
    // Event listeners for TLS settings
    requireTlsCheckbox.addEventListener('change', toggleTlsSettings);
    tlsDisableCertsCheckbox.addEventListener('change', toggleCertificateFields);
    
    // Initialize TLS settings visibility
    toggleTlsSettings();
    toggleCertificateFields();
    
    // Load saved configuration on page load
    const savedConfig = localStorage.getItem('mqttConfig');
    if (savedConfig) {
        const config = JSON.parse(savedConfig);
        Object.keys(config).forEach(key => {
            const element = document.getElementById(key);
            if (element) {
                if (element.type === 'checkbox') {
                    element.checked = config[key] === 'on';
                } else {
                    element.value = config[key];
                }
            }
        });
        // Re-initialize TLS settings after loading config
        toggleTlsSettings();
        toggleCertificateFields();
    }
});

// MQTT Message Search Functionality
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('searchInput');
    const tableBody = document.getElementById('messageTableBody');
    const rows = tableBody.getElementsByTagName('tr');
    
    // Convert HTMLCollection to Array and filter out the "no results" row
    const rowsArray = Array.from(rows).filter(row => row.id !== 'noResultsRow');
    
    // Function to update stats counters based on visible rows
    function updateStatsCounters(visibleRows) {
        let critical = 0;
        let processing = 0; 
        let processed = 0;
        
        visibleRows.forEach(function(row) {
            const severity = row.getAttribute('data-severity') || '';
            const status = row.getAttribute('data-status') || '';
            
            // Count critical messages that are not resolved
            if (severity.toLowerCase() === 'critical' && status !== 'resolved') {
                critical++;
            }
            // Count in progress messages
            if (status === 'in_progress') {
                processing++;
            }
            // Count resolved messages
            if (status === 'resolved') {
                processed++;
            }
        });
        
        // Update the counter displays
        const criticalElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-error');
        const processingElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-warning');
        const processedElement = document.querySelector('.text-2xl.font-semibold.text-mqtt-success');
        
        if (criticalElement) criticalElement.textContent = critical;
        if (processingElement) processingElement.textContent = processing;
        if (processedElement) processedElement.textContent = processed;
    }
    
    searchInput.addEventListener('input', function() {
        const searchTerm = this.value.toLowerCase().trim();
        console.log('🔍 Search triggered with term:', searchTerm);
        
        rowsArray.forEach(function(row) {
            if (searchTerm === '') {
                // Show all rows when search is empty
                row.style.display = '';
            } else {
                // Get searchable data from row attributes (only topic/source)
                const topic = row.getAttribute('data-topic') || '';
                
                // Search only in the topic/source field
                const searchableText = topic;
                
                // Debug logging for any search term
                if (searchTerm.length > 0) {
                    console.log('Row topic: ' + topic + ', searchTerm: ' + searchTerm + ', match: ' + searchableText.includes(searchTerm));
                }
                // Show/hide row based on search match
                if (searchableText.includes(searchTerm)) {
                    row.style.display = '';
                } else {
                    row.style.display = 'none';
                }
            }
        });
        
        // Update counters for empty search (show all data)
        if (searchTerm === '') {
            updateStatsCounters(rowsArray);
        }
        // Update search results count and show/hide no results message
        const visibleRows = rowsArray.filter(row => row.style.display !== 'none').length;
        const noResultsRow = document.getElementById('noResultsRow');
        
        if (searchTerm !== '' && visibleRows === 0) {
            noResultsRow.style.display = '';
        } else {
            noResultsRow.style.display = 'none';
        }
        // Update stats counters based on visible rows
        updateStatsCounters(rowsArray.filter(row => row.style.display !== 'none'));
        
        console.log(`Search: "${searchTerm}" - ${visibleRows} results found`);
    });
    
    // Add search shortcut (Ctrl/Cmd + K)
    document.addEventListener('keydown', function(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
            e.preventDefault();
            searchInput.focus();
            searchInput.select();
        }
    });
    
    console.log('🔍 MQTT Search functionality initialized');
    console.log('💡 Tip: Use Ctrl/Cmd + K to focus search box');
    console.log('📊 Found ' + rowsArray.length + ' searchable rows');
    
    // Test search functionality immediately
    console.log('🧪 Testing search functionality...');
    rowsArray.forEach(function(row, index) {
        const topic = row.getAttribute('data-topic') || '';
        console.log('Row ' + (index + 1) + ' topic: ' + topic);
    });
});
