// ==================== DOM ELEMENTS ====================
const sidebar = document.getElementById('sidebar');
const sidebarOverlay = document.getElementById('sidebarOverlay');
const btnToggleSidebar = document.getElementById('btnToggleSidebar');
// const btnCloseSidebar = document.getElementById('btnCloseSidebar');
const btnNewChatSidebar = document.getElementById('btnNewChatSidebar');
const historyList = document.getElementById('historyList');
const messagesWrapper = document.getElementById('messagesWrapper');
const inputMessage = document.getElementById('inputMessage');
const btnSend = document.getElementById('btnSend');
const mainContent = document.querySelector('.main-content');

// ==================== STATE ====================
let isLoading = false;
let currentChatId = null;
let chatHistory = [];

// ==================== INITIALIZATION ====================
function initApp() {
    console.log('🚀 Initializing Jewelry Sales Chatbot...');
    loadChatHistory();
    showEmptyState();
    attachEventListeners();
    
    // Check screen size and close sidebar on mobile
    if (window.innerWidth <= 768) {
        closeSidebar();
    }
}

function attachEventListeners() {
    btnToggleSidebar?.addEventListener('click', toggleSidebar);
    // btnCloseSidebar?.addEventListener('click', closeSidebar);
    sidebarOverlay?.addEventListener('click', closeSidebar);
    btnNewChatSidebar?.addEventListener('click', handleNewChat);
    btnSend?.addEventListener('click', handleSend);
    inputMessage?.addEventListener('keydown', handleInputKeydown);
    inputMessage?.addEventListener('input', autoResizeTextarea);
}

// Initialize on load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}

// ==================== SIDEBAR FUNCTIONS ====================
function toggleSidebar() {
    const isClosed = sidebar.classList.contains('closed');
    if (isClosed) {
        openSidebar();
    } else {
        closeSidebar();
    }
}

function closeSidebar() {
    sidebar.classList.add('closed');
    sidebar.classList.remove('open');
    mainContent.classList.add('sidebar-closed');
    sidebarOverlay.classList.remove('active');
}

function openSidebar() {
    sidebar.classList.remove('closed');
    sidebar.classList.add('open');
    mainContent.classList.remove('sidebar-closed');
    
    if (window.innerWidth <= 768) {
        sidebarOverlay.classList.add('active');
    }
}

// ==================== CHAT HISTORY ====================
function loadChatHistory() {
    const saved = localStorage.getItem('jewelryChatHistory');
    if (saved) {
        try {
            chatHistory = JSON.parse(saved);
        } catch (e) {
            console.error('Error parsing chat history:', e);
            chatHistory = [];
        }
    } else {
        // Demo data
        chatHistory = [
            {
                id: 'demo-1',
                title: 'Analisis Penjualan Bulan Ini',
                timestamp: new Date(Date.now() - 3600000).toISOString(),
                messages: []
            },
            {
                id: 'demo-2',
                title: 'Ringkasan Per Produk',
                timestamp: new Date(Date.now() - 86400000).toISOString(),
                messages: []
            }
        ];
    }
    renderChatHistory();
}

function saveChatHistory() {
    try {
        localStorage.setItem('jewelryChatHistory', JSON.stringify(chatHistory));
    } catch (e) {
        console.error('Error saving chat history:', e);
    }
}

function renderChatHistory() {
    if (!historyList) return;
    
    if (chatHistory.length === 0) {
        historyList.innerHTML = '<div style="padding: 20px; text-align: center; color: rgba(255,255,255,0.5); font-size: 13px;">Belum ada riwayat chat</div>';
        return;
    }
    
    historyList.innerHTML = chatHistory.map(chat => `
        <div class="history-item ${chat.id === currentChatId ? 'active' : ''}" data-chat-id="${chat.id}">
            <div class="history-item-content">
                <div class="history-item-title">${escapeHtml(chat.title)}</div>
                <div class="history-item-time">${formatTimeAgo(chat.timestamp)}</div>
            </div>
            <div class="history-item-actions">
                <button class="btn-history-action btn-rename" title="Rename" data-chat-id="${chat.id}">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                        <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                    </svg>
                </button>
                <button class="btn-history-action btn-delete" title="Delete" data-chat-id="${chat.id}">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="3 6 5 6 21 6"/>
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                    </svg>
                </button>
            </div>
        </div>
    `).join('');
    
    // Attach event listeners
    document.querySelectorAll('.history-item').forEach(item => {
        item.addEventListener('click', (e) => {
            if (!e.target.closest('.btn-history-action')) {
                loadChat(item.dataset.chatId);
            }
        });
    });
    
    document.querySelectorAll('.btn-rename').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            renameChat(btn.dataset.chatId);
        });
    });
    
    document.querySelectorAll('.btn-delete').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            deleteChat(btn.dataset.chatId);
        });
    });
}

function formatTimeAgo(timestamp) {
    const now = new Date();
    const then = new Date(timestamp);
    const diffMs = now - then;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'Baru saja';
    if (diffMins < 60) return `${diffMins} menit lalu`;
    if (diffHours < 24) return `${diffHours} jam lalu`;
    if (diffDays < 7) return `${diffDays} hari lalu`;
    
    return then.toLocaleDateString('id-ID', { day: 'numeric', month: 'short' });
}

function createNewChat(firstMessage = null) {
    const newChat = {
        id: 'chat-' + Date.now(),
        title: firstMessage ? firstMessage.substring(0, 50) : 'Chat Baru',
        timestamp: new Date().toISOString(),
        messages: []
    };
    
    chatHistory.unshift(newChat);
    currentChatId = newChat.id;
    saveChatHistory();
    renderChatHistory();
    
    return newChat;
}

function loadChat(chatId) {
    const chat = chatHistory.find(c => c.id === chatId);
    if (!chat) return;
    
    currentChatId = chatId;
    messagesWrapper.innerHTML = '';
    
    if (chat.messages.length === 0) {
        showEmptyState();
    } else {
        chat.messages.forEach(msg => {
            addMessage(msg.text, msg.role, msg.data, msg.totalCount);
        });
    }
    
    renderChatHistory();
    
    if (window.innerWidth <= 768) {
        closeSidebar();
    }
}

function renameChat(chatId) {
    const chat = chatHistory.find(c => c.id === chatId);
    if (!chat) return;
    
    const newTitle = prompt('Masukkan nama baru:', chat.title);
    if (newTitle && newTitle.trim()) {
        chat.title = newTitle.trim();
        saveChatHistory();
        renderChatHistory();
    }
}

function deleteChat(chatId) {
    if (!confirm('Hapus riwayat chat ini?')) return;
    
    chatHistory = chatHistory.filter(c => c.id !== chatId);
    
    if (currentChatId === chatId) {
        currentChatId = null;
        messagesWrapper.innerHTML = '';
        showEmptyState();
    }
    
    saveChatHistory();
    renderChatHistory();
}

function getCurrentChat() {
    if (!currentChatId) {
        return createNewChat();
    }
    return chatHistory.find(c => c.id === currentChatId);
}

function saveChatMessage(text, role, data = null, totalCount = null) {
    const chat = getCurrentChat();
    if (!chat) return;
    
    chat.messages.push({ text, role, data, totalCount });
    chat.timestamp = new Date().toISOString();
    
    // Update title from first user message
    if (role === 'user' && chat.messages.filter(m => m.role === 'user').length === 1) {
        chat.title = text.substring(0, 50) + (text.length > 50 ? '...' : '');
    }
    
    saveChatHistory();
    renderChatHistory();
}

// ==================== MESSAGE HANDLING ====================
function showEmptyState() {
    messagesWrapper.innerHTML = `
        <div class="empty-state">
            <div class="empty-logo">💎</div>
            <h2 class="empty-title">Jewelry Sales AI</h2>
            <p class="empty-subtitle">Dapatkan insight mendalam tentang penjualan perhiasan Anda</p>
            <div class="quick-actions">
                <button class="action-card" onclick="sendQuickAction('Ringkasan penjualan per produk')">
                    <div class="action-icon">📊</div>
                    <span>Ringkasan per produk</span>
                </button>
                <button class="action-card" onclick="sendQuickAction('Penjualan bulan ini')">
                    <div class="action-icon">📈</div>
                    <span>Penjualan bulan ini</span>
                </button>
                <button class="action-card" onclick="sendQuickAction('Analisis per lokasi')">
                    <div class="action-icon">📍</div>
                    <span>Analisis per lokasi</span>
                </button>
                <button class="action-card" onclick="sendQuickAction('Transaksi terbaru')">
                    <div class="action-icon">🔔</div>
                    <span>Transaksi terbaru</span>
                </button>
            </div>
        </div>
    `;
}

function addMessage(text, role, data = null, totalCount = null) {
    // Remove empty state if it exists
    const emptyState = messagesWrapper.querySelector('.empty-state');
    if (emptyState) {
        emptyState.remove();
    }
    
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
            if (tableEl) {
                bubbleEl.appendChild(tableEl);
            }
        }
    } else {
        bubbleEl.textContent = text;
    }
    
    messageEl.appendChild(bubbleEl);
    messagesWrapper.appendChild(messageEl);
    
    setTimeout(() => {
        messagesWrapper.parentElement.scrollTop = messagesWrapper.parentElement.scrollHeight;
    }, 100);
}

function buildTable(data, totalCount) {
    if (!Array.isArray(data) || data.length === 0) return null;
    
    const scrollDiv = document.createElement('div');
    scrollDiv.className = 'table-scroll';
    
    const table = document.createElement('table');
    table.className = 'data-table';
    
    // Headers
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    Object.keys(data[0]).forEach(key => {
        const th = document.createElement('th');
        th.textContent = key;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Body
    const tbody = document.createElement('tbody');
    data.forEach(row => {
        const tr = document.createElement('tr');
        Object.values(row).forEach(val => {
            const td = document.createElement('td');
            td.textContent = val;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    
    scrollDiv.appendChild(table);
    
    if (totalCount && totalCount > data.length) {
        const moreDiv = document.createElement('div');
        moreDiv.className = 'table-more';
        moreDiv.textContent = `Menampilkan ${data.length} dari ${totalCount} data`;
        scrollDiv.appendChild(moreDiv);
    }
    
    return scrollDiv;
}

function addTypingIndicator() {
    const messageEl = document.createElement('div');
    messageEl.className = 'message assistant';
    messageEl.id = 'typing-indicator';
    
    const bubbleEl = document.createElement('div');
    bubbleEl.className = 'bubble assistant';
    
    const typingDiv = document.createElement('div');
    typingDiv.className = 'typing-indicator';
    typingDiv.innerHTML = `
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
        <div class="typing-dot"></div>
    `;
    
    bubbleEl.appendChild(typingDiv);
    messageEl.appendChild(bubbleEl);
    messagesWrapper.appendChild(messageEl);
    
    setTimeout(() => {
        messagesWrapper.parentElement.scrollTop = messagesWrapper.parentElement.scrollHeight;
    }, 100);
}

function removeTypingIndicator() {
    const indicator = document.getElementById('typing-indicator');
    if (indicator) {
        indicator.remove();
    }
}

// ==================== USER INPUT ====================
function autoResizeTextarea() {
    inputMessage.style.height = 'auto';
    inputMessage.style.height = Math.min(inputMessage.scrollHeight, 150) + 'px';
}

function handleInputKeydown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
    }
}

function handleNewChat() {
    currentChatId = null;
    messagesWrapper.innerHTML = '';
    showEmptyState();
    inputMessage.value = '';
    autoResizeTextarea();
    inputMessage.focus();
    
    if (window.innerWidth <= 768) {
        closeSidebar();
    }
}

function handleSend() {
    const query = inputMessage.value.trim();
    if (!query || isLoading) return;
    
    // Clear input
    inputMessage.value = '';
    autoResizeTextarea();
    
    // Add user message
    addMessage(query, 'user');
    saveChatMessage(query, 'user');
    
    // Send to backend
    sendQuery(query);
}

function sendQuickAction(text) {
    inputMessage.value = text;
    handleSend();
}

async function sendQuery(query) {
    isLoading = true;
    btnSend.disabled = true;

    addTypingIndicator();

    try {
        const response = await fetch(`/chat?query=${encodeURIComponent(query)}`);

        if (!response.ok) {
            throw new Error('Network response was not ok');
        }

        const result = await response.json();

        removeTypingIndicator();

        const text = result.message || 'Tidak ada jawaban.';
        const data = result.data || null;
        const totalCount = result.total_count || null;

        addMessage(text, 'assistant', data, totalCount);
        saveChatMessage(text, 'assistant', data, totalCount);

    } catch (error) {
        console.error('Error:', error);
        removeTypingIndicator();
        addMessage('Maaf, terjadi kesalahan saat memproses permintaan Anda.', 'assistant');
    } finally {
        isLoading = false;
        btnSend.disabled = false;
        inputMessage.focus();
    }
}


// ==================== UTILITY FUNCTIONS ====================
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}