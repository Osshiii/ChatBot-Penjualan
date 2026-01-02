// DOM Elements
const messagesWrapper = document.getElementById('messagesWrapper');
const inputMessage = document.getElementById('inputMessage');
const btnSend = document.getElementById('btnSend');
const btnNewChat = document.getElementById('btnNewChat');

console.log('Script loaded');
console.log('Elements:', { messagesWrapper, inputMessage, btnSend, btnNewChat });

// State
let isLoading = false;
let lastQueryRequestedAnalysis = false;

// Initialize immediately
function initApp() {
    console.log('initApp called');
    showEmptyState();
    
    // Attach event listeners
    if (btnSend) {
        btnSend.onclick = handleSend;
        console.log('btnSend onclick attached');
    }
    
    if (btnNewChat) {
        btnNewChat.onclick = handleNewChat;
        console.log('btnNewChat onclick attached');
    }
    
    if (inputMessage) {
        inputMessage.onkeydown = handleInputKeydown;
        console.log('inputMessage onkeydown attached');
    }
}

// Run on load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}

// Show empty state
function showEmptyState() {
    console.log('showEmptyState called');
    messagesWrapper.innerHTML = `
        <div class="empty-state">
            <div class="empty-icon">💎</div>
            <h2 class="empty-title">Jewelry Sales AI</h2>
            <p class="empty-subtitle">Mulai bertanya tentang penjualan perhiasan</p>
            <div class="quick-chips">
                <button class="chip" onclick="sendChip('Ringkasan penjualan per produk')">Ringkasan per produk</button>
                <button class="chip" onclick="sendChip('Penjualan bulan ini')">Penjualan bulan ini</button>
                <button class="chip" onclick="sendChip('Data per lokasi')">Data per lokasi</button>
                <button class="chip" onclick="sendChip('Penjualan terbaru')">Penjualan terbaru</button>
            </div>
        </div>
    `;
}

// Add message to chat
function addMessage(text, role, data = null, totalCount = null) {
    console.log('addMessage:', { text, role, data });
    const messageEl = document.createElement('div');
    messageEl.className = `message ${role}`;
    
    const bubbleEl = document.createElement('div');
    bubbleEl.className = `bubble ${role}`;
    
    if (role === 'assistant') {
        const textDiv = document.createElement('div');
        textDiv.className = 'message-text';
        textDiv.textContent = text;
        bubbleEl.appendChild(textDiv);
        
        if (data && data.length > 0) {
            const tableEl = buildTable(data, totalCount);
            bubbleEl.appendChild(tableEl);
        }
    } else {
        bubbleEl.textContent = text;
    }
    
    messageEl.appendChild(bubbleEl);
    messagesWrapper.appendChild(messageEl);
    
    // Scroll to bottom
    setTimeout(() => {
        messagesWrapper.scrollTop = messagesWrapper.scrollHeight;
    }, 0);
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Build table DOM element for data
function buildTable(rows, totalCount) {
    if (!rows || rows.length === 0) return null;

    const cols = Object.keys(rows[0]);
    
    const wrapper = document.createElement('div');
    wrapper.className = 'table-scroll';
    
    const table = document.createElement('table');
    table.className = 'data-table';
    
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    cols.forEach(c => {
        const th = document.createElement('th');
        th.textContent = c;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    const tbody = document.createElement('tbody');
    rows.forEach(r => {
        const row = document.createElement('tr');
        cols.forEach(c => {
            const td = document.createElement('td');
            td.textContent = r[c] ?? '';
            row.appendChild(td);
        });
        tbody.appendChild(row);
    });
    table.appendChild(tbody);
    
    wrapper.appendChild(table);
    if (typeof totalCount === "number" && totalCount > rows.length) {
        const more = totalCount - rows.length;
        const note = document.createElement("div");
        note.className = "table-more";
        note.textContent = `and ${more.toLocaleString()} more…`;
        wrapper.appendChild(note);
    }
    return wrapper;
}

function getPayload(data) {
    return data?.response ?? data;
}

// Check if user asked for analysis
function userAskedForAnalysis(message) {
    const msg = message.toLowerCase();
    const keywords = [
        "ringkasan", "insight", "analisis", "saran", "rekomendasi",
        "kesimpulan", "jelaskan", "kenapa", "bandingkan"
    ];
    return keywords.some(kw => msg.includes(kw));
}

// Send message
async function handleSend() {
    console.log('handleSend called');
    const text = inputMessage.value.trim();
    console.log('Message text:', text);
    
    if (!text || isLoading) {
        console.log('Aborting: no text or loading');
        return;
    }
    
    lastQueryRequestedAnalysis = userAskedForAnalysis(text);
    
    isLoading = true;
    btnSend.disabled = true;
    
    // Remove empty state if present
    const emptyState = messagesWrapper.querySelector('.empty-state');
    if (emptyState) {
        messagesWrapper.innerHTML = '';
    }
    
    console.log('Adding user message:', text);
    addMessage(text, 'user');
    inputMessage.value = '';
    inputMessage.focus();
    
    try {
        console.log('Sending request to /chat');
        const response = await fetch('/chat?query=' + encodeURIComponent(text), {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });
        
        console.log('Response status:', response.status);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        console.log('Response data:', data);

        const payload = getPayload(data);

        let replyText = 'Maaf, tidak ada respons dari server.';

        if (typeof payload === 'string') {
            replyText = payload;
        } else if (payload?.message) {
            replyText = payload.message;
        } else if (payload && typeof payload === 'object') {
            replyText = JSON.stringify(payload, null, 2);
        }

        console.log('Adding bot message:', replyText);
        
        // Determine if we should show table
        const shouldShowTable = 
            payload?.data && 
            payload.data.length > 0 && 
            !lastQueryRequestedAnalysis;
        
        const totalCount = payload?.count ?? payload?.total_records ?? null;
        
        addMessage(
            replyText,
            'assistant',
            shouldShowTable ? payload.data : null,
            totalCount
        );

    } catch (error) {
        console.error('Error:', error);
        addMessage(`❌ Error: ${error.message}`, 'assistant');
    } finally {
        isLoading = false;
        btnSend.disabled = false;
        inputMessage.focus();
    }
}

// Handle input keydown
function handleInputKeydown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
    }
}

// Send from chip
function sendChip(text) {
    inputMessage.value = text;
    inputMessage.focus();
    handleSend();
}

// New chat
function handleNewChat() {
    inputMessage.value = '';
    isLoading = false;
    btnSend.disabled = false;
    lastQueryRequestedAnalysis = false;
    showEmptyState();
    inputMessage.focus();
}