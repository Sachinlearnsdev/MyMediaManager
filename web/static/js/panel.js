// MyMediaManager Control Panel - Main JS

// Global Socket.IO connection
window._socket = null;

// ── Toast Notifications ──
window.toast = {
    _show(msg, bg, duration) {
        Toastify({
            text: msg,
            duration: duration || 4000,
            gravity: 'bottom',
            position: 'right',
            style: { background: bg, borderRadius: '8px', fontFamily: 'inherit', fontSize: '0.85rem', padding: '10px 18px', boxShadow: '0 4px 16px rgba(0,0,0,0.25)' },
            stopOnFocus: true,
        }).showToast();
    },
    success(msg, ms) { this._show(msg, 'linear-gradient(135deg, #22c55e, #16a34a)', ms); },
    error(msg, ms)   { this._show(msg, 'linear-gradient(135deg, #ef4444, #dc2626)', ms); },
    warn(msg, ms)    { this._show(msg, 'linear-gradient(135deg, #f59e0b, #d97706)', ms); },
    info(msg, ms)    { this._show(msg, 'linear-gradient(135deg, #3b82f6, #2563eb)', ms); },
};

function panelApp() {
    return {
        darkMode: true,
        sidebarCollapsed: false,

        init() {
            // Initialize Socket.IO
            window._socket = io({ transports: ['polling'] });

            window._socket.on('connect', () => {
                console.log('[MMM] Connected to server');
            });

            window._socket.on('disconnect', () => {
                console.log('[MMM] Disconnected from server');
            });

            // Theme persistence
            const saved = localStorage.getItem('mmm-theme');
            if (saved) {
                this.darkMode = saved === 'dark';
                document.documentElement.setAttribute('data-theme', saved);
            }

            // Sidebar persistence
            const sidebarState = localStorage.getItem('mmm-sidebar');
            if (sidebarState === 'collapsed') {
                this.sidebarCollapsed = true;
            }

            // Auto-collapse sidebar on mobile
            if (window.innerWidth <= 992) {
                this.sidebarCollapsed = true;
            }
        },

        toggleTheme() {
            this.darkMode = !this.darkMode;
            const theme = this.darkMode ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', theme);
            localStorage.setItem('mmm-theme', theme);
        },

        toggleSidebar() {
            this.sidebarCollapsed = !this.sidebarCollapsed;
            localStorage.setItem('mmm-sidebar', this.sidebarCollapsed ? 'collapsed' : 'expanded');
        },
    };
}
