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

// Delete confirmation state
let deleteConfirmState = {
    isConfirmOpen: false,
    isDeleting: false,
    chatIdToDelete: null,
    deleteStatus: 'idle', // 'idle' | 'success' | 'error'
    deleteErrorMessage: null
};

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
    
    // Delete modal listeners
    const deleteModalOverlay = document.getElementById('deleteModalOverlay');
    const deleteConfirmModal = document.getElementById('deleteConfirmModal');
    const deleteModalClose = document.getElementById('deleteModalClose');
    const deleteModalCancel = document.getElementById('deleteModalCancel');
    const deleteModalConfirm = document.getElementById('deleteModalConfirm');
    
    deleteModalOverlay?.addEventListener('click', closeDeleteModal);
    deleteModalClose?.addEventListener('click', closeDeleteModal);
    deleteModalCancel?.addEventListener('click', closeDeleteModal);
    deleteModalConfirm?.addEventListener('click', handleDeleteConfirm);
    
    // ESC key to close modal
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && deleteConfirmState.isConfirmOpen) {
            closeDeleteModal();
        }
    });
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
    console.log('🗑️ deleteChat called with chatId:', chatId);
    deleteConfirmState.isConfirmOpen = true;
    deleteConfirmState.chatIdToDelete = chatId;
    deleteConfirmState.deleteStatus = 'idle';
    deleteConfirmState.deleteErrorMessage = null;
    
    openDeleteModal();
}

function openDeleteModal() {
    console.log('📂 openDeleteModal called');
    const modal = document.getElementById('deleteConfirmModal');
    const overlay = document.getElementById('deleteModalOverlay');
    const confirmBtn = document.getElementById('deleteModalConfirm');
    const errorDiv = document.getElementById('deleteErrorMessage');
    
    console.log('Modal element:', modal);
    console.log('Overlay element:', overlay);
    
    if (modal && overlay) {
        modal.style.display = 'block';
        overlay.style.display = 'block';
        errorDiv.style.display = 'none';
        confirmBtn.disabled = false;
        
        // Focus confirm button
        confirmBtn.focus();
        
        // Prevent body scroll
        document.body.style.overflow = 'hidden';
        console.log('✅ Modal opened successfully');
    } else {
        console.error('❌ Modal or overlay element not found');
    }
}

function closeDeleteModal() {
    console.log('🚪 closeDeleteModal called');
    const modal = document.getElementById('deleteConfirmModal');
    const overlay = document.getElementById('deleteModalOverlay');
    
    if (modal && overlay) {
        modal.style.display = 'none';
        overlay.style.display = 'none';
        deleteConfirmState.isConfirmOpen = false;
        deleteConfirmState.chatIdToDelete = null;
        deleteConfirmState.deleteStatus = 'idle';
        deleteConfirmState.deleteErrorMessage = null;
        
        // Restore body scroll
        document.body.style.overflow = '';
        console.log('✅ Modal closed and state reset');
    } else {
        console.error('❌ Modal or overlay element not found');
    }
}

async function handleDeleteConfirm() {
    console.log('✋ handleDeleteConfirm called, isDeleting:', deleteConfirmState.isDeleting);
    const chatId = deleteConfirmState.chatIdToDelete;
    if (!chatId) {
        console.error('No chat ID to delete');
        return;
    }
    
    // Guard: prevent double-click
    if (deleteConfirmState.isDeleting) {
        console.warn('⚠️ Already deleting, ignoring duplicate click');
        return;
    }
    
    deleteConfirmState.isDeleting = true;
    console.log('🔒 Set isDeleting = true, protecting against double-click');
    
    const confirmBtn = document.getElementById('deleteModalConfirm');
    const cancelBtn = document.getElementById('deleteModalCancel');
    const btnText = confirmBtn.querySelector('.btn-text');
    const btnLoader = confirmBtn.querySelector('.btn-loader');
    const errorDiv = document.getElementById('deleteErrorMessage');
    
    try {
        console.log('⏳ Starting deletion of chat:', chatId);
        
        // Show loading state
        confirmBtn.disabled = true;
        cancelBtn.disabled = true;
        btnText.style.display = 'none';
        btnLoader.style.display = 'inline';
        errorDiv.style.display = 'none';
        console.log('🔄 Loading state shown on buttons');
        
        // Delete from localStorage (synchronous operation)
        chatHistory = chatHistory.filter(c => c.id !== chatId);
        console.log('🗑️ Chat deleted from array, remaining:', chatHistory.length);
        
        // Reset current chat if it was deleted
        if (currentChatId === chatId) {
            currentChatId = null;
            messagesWrapper.innerHTML = '';
            showEmptyState();
            console.log('📭 Current chat was deleted, showing empty state');
        }
        
        // Persist to storage
        saveChatHistory();
        renderChatHistory();
        console.log('💾 Chat history saved and re-rendered');
        
        // Close modal first
        const modal = document.getElementById('deleteConfirmModal');
        const overlay = document.getElementById('deleteModalOverlay');
        if (modal && overlay) {
            modal.style.display = 'none';
            overlay.style.display = 'none';
            console.log('✅ Modal and overlay hidden');
        }
        
        // Reset modal state
        deleteConfirmState.isConfirmOpen = false;
        deleteConfirmState.deleteStatus = 'success';
        console.log('✅ Modal state reset to closed');
        
        // Show success toast
        console.log('🎉 Calling showDeleteSuccess()');
        showDeleteSuccess();
        
    } catch (error) {
        console.error('❌ Error during deletion:', error);
        deleteConfirmState.deleteStatus = 'error';
        deleteConfirmState.deleteErrorMessage = error.message || 'Gagal menghapus riwayat chat';
        
        // Show error in modal
        errorDiv.textContent = deleteConfirmState.deleteErrorMessage;
        errorDiv.style.display = 'block';
        console.log('⚠️ Error displayed in modal');
        
        // Show error toast
        showToast(deleteConfirmState.deleteErrorMessage, 'error', 5000);
        
    } finally {
        // ALWAYS reset button state (whether success or error)
        console.log('🔓 finally block: resetting button state...');
        deleteConfirmState.isDeleting = false;
        
        // Restore buttons to normal state
        if (deleteConfirmState.deleteStatus !== 'success') {
            confirmBtn.disabled = false;
            cancelBtn.disabled = false;
            btnText.style.display = 'inline';
            btnLoader.style.display = 'none';
            console.log('🔘 Buttons enabled (for retry)');
        } else {
            // Success case: keep buttons disabled until modal fully closed
            console.log('✨ Success case: buttons stay disabled, modal is closed');
        }
    }
}

function showDeleteSuccess() {
    showToast('Berhasil dihapus', 'success', 3000);
}

function showToast(message, type = 'info', duration = 3000) {
    console.log('🍞 showToast called:', message, type);
    const container = document.getElementById('toastContainer');
    if (!container) {
        console.error('❌ Toast container not found');
        return;
    }
    
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    toast.role = 'status';
    toast.setAttribute('aria-live', 'polite');
    
    container.appendChild(toast);
    console.log('✅ Toast element created and appended');
    
    // Trigger animation
    setTimeout(() => {
        toast.classList.add('show');
        console.log('Animation triggered');
    }, 10);
    
    // Remove after duration
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            toast.remove();
        }, 300);
    }, duration);
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