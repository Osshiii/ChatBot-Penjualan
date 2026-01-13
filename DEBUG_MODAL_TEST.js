// Test to verify modal functionality
// Copy & paste this in browser console:

console.log('=== Modal Debug Test ===');

// Check if elements exist
const modal = document.getElementById('deleteConfirmModal');
const overlay = document.getElementById('deleteModalOverlay');
const closeBtn = document.getElementById('deleteModalClose');
const cancelBtn = document.getElementById('deleteModalCancel');
const confirmBtn = document.getElementById('deleteModalConfirm');

console.log('Modal exists:', !!modal);
console.log('Overlay exists:', !!overlay);
console.log('Close button exists:', !!closeBtn);
console.log('Cancel button exists:', !!cancelBtn);
console.log('Confirm button exists:', !!confirmBtn);

// Check if functions exist
console.log('deleteChat function exists:', typeof deleteChat === 'function');
console.log('openDeleteModal function exists:', typeof openDeleteModal === 'function');
console.log('closeDeleteModal function exists:', typeof closeDeleteModal === 'function');

// Try to manually open modal
console.log('Testing manual open...');
if (modal && overlay) {
    modal.style.display = 'block';
    overlay.style.display = 'block';
    console.log('✅ Modal should be visible now');
} else {
    console.error('❌ Modal or overlay not found');
}

// Check delete state
console.log('deleteConfirmState:', window.deleteConfirmState);
