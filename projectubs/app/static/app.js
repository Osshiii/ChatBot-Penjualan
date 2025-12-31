// DOM Elements
const messagesWrapper = document.getElementById('messagesWrapper');
const inputMessage = document.getElementById('inputMessage');
const btnSend = document.getElementById('btnSend');
const btnNewChat = document.getElementById('btnNewChat');

console.log('Script loaded');
console.log('Elements:', { messagesWrapper, inputMessage, btnSend, btnNewChat });

// State
let isLoading = false;

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
function addMessage(text, role) {
    console.log('addMessage:', { text, role });
    const messageEl = document.createElement('div');
    messageEl.className = `message ${role}`;
    
    const bubbleEl = document.createElement('div');
    bubbleEl.className = `bubble ${role}`;
    bubbleEl.textContent = text;
    
    messageEl.appendChild(bubbleEl);
    messagesWrapper.appendChild(messageEl);
    
    // Scroll to bottom
    setTimeout(() => {
        messagesWrapper.scrollTop = messagesWrapper.scrollHeight;
    }, 0);
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
        
        let replyText = 'Maaf, tidak ada respons dari server.';
        
        // Handle berbagai format response
        if (data.response) {
            if (typeof data.response === 'string') {
                replyText = data.response;
            } else if (data.response.message) {
                replyText = data.response.message;
            } else if (typeof data.response === 'object') {
                // Jika response adalah object tapi bukan message, stringify
                replyText = JSON.stringify(data.response, null, 2);
            }
        }
        
        console.log('Adding bot message:', replyText);
        addMessage(replyText, 'assistant');
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
    showEmptyState();
    inputMessage.focus();
}