import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInAnonymously, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, doc, addDoc, getDocs, onSnapshot, deleteDoc, query, where, writeBatch, orderBy, updateDoc } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";

// --- Main Application Module (IIFE) ---
const App = (() => {
    // --- State Management ---
    const state = {
        db: null,
        auth: null,
        storage: null,
        currentUser: null,
        users: [],
        games: [],
        selectedPlayers: [], // {id, name, photoURL}
        hanchanScores: [],   // Holds scores for each hanchan
        activeInputId: null, // Holds the ID of the active score input in the modal
        editingGameId: null, // ID of the game being edited
        cachedStats: {},
        playerTrophies: {}, // { playerId: { trophyId: true, ... } }
        charts: {
            playerRadar: null,
            pointHistory: null,
            playerBar: null,
            personalRank: null,
            personalPointHistory: null
        }
    };

    // --- Constants ---
    const CONSTANTS = {
        YAKUMAN_LIST: [
            '国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '天和', '地和',
            '国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'
        ],
        PENALTY_REASONS: {
            chombo: ['誤ロン・誤ツモ', 'ノーテンリーチ', 'その他'],
            agariHouki: ['多牌・少牌', '喰い替え', 'その他']
        },
        YAKUMAN_INCOMPATIBILITY: {
            '天和': ['国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '地和', '国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'],
            '地和': ['国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '天和', '国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'],
            '国士無双': ['四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '天和', '地和', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'],
            '国士無双十三面待ち': ['四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '天和', '地和', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'],
            // ... (add all other incompatibilities from original code)
        },
        FIREBASE_CONFIG: {
            apiKey: "AIzaSyBwWqWxRy5JlcQwbc5KAXRvH0swd0pOzSg",
            authDomain: "edogawa-m-league-summary.firebaseapp.com",
            projectId: "edogawa-m-league-summary",
            storageBucket: "edogawa-m-league-summary.appspot.com",
            messagingSenderId: "587593171009",
            appId: "1:587593171009:web:b48dd5b809f2d2ce8886c0",
            measurementId: "G-XMYXPG06QF"
        }
    };
    
    // --- DOM Element References ---
    const DOMElements = {
        app: document.getElementById('app'),
        authStatus: document.getElementById('auth-status'),
        tabNavigation: document.getElementById('tab-navigation'),
        mainContent: document.getElementById('main-content'),
        modal: document.getElementById('modal'),
        modalContent: document.getElementById('modal-content'),
        // Add other frequently accessed elements here
    };

    // --- Helper & Calculation Functions ---
    // ... (All calculation functions like getGameYears, calculateAllPlayerStats, etc. go here)
    // Example:
    function getGameYears() {
        const years = new Set();
        state.games.forEach(game => {
            const dateStr = game.gameDate;
            if (dateStr) {
                const year = dateStr.substring(0, 4);
                if (!isNaN(year)) years.add(year);
            } else if (game.createdAt && game.createdAt.seconds) {
                const year = new Date(game.createdAt.seconds * 1000).getFullYear().toString();
                years.add(year);
            }
        });
        return Array.from(years).sort((a, b) => b - a);
    }

    // --- UI Rendering Functions ---
    // ... (All render functions like renderGameTab, renderLeaderboardTab, etc. go here)
    // Example:
    function renderGameTab() {
        const container = document.getElementById('game-tab');
        if(!container) return;
        container.innerHTML = `
            <div id="step1-player-selection" class="cyber-card p-4 sm:p-6">
                <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 1: 雀士選択</h2>
                <div id="player-list-for-selection" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4"></div>
                <div class="flex justify-end">
                    <button id="to-step2-btn" class="cyber-btn px-6 py-2 rounded-lg" disabled>進む <i class="fas fa-arrow-right ml-2"></i></button>
                </div>
            </div>
            <div id="step2-rule-settings" class="cyber-card p-4 sm:p-6 hidden">
                <!-- ... Step 2 HTML ... -->
            </div>
            <div id="step3-score-input" class="cyber-card p-4 sm:p-6 hidden">
                <!-- ... Step 3 HTML ... -->
            </div>
        `;
        renderPlayerSelection();
    }
    
    function renderPlayerSelection() {
        // ... implementation
    }

    // --- Event Handlers ---
    function handleTabClick(e) {
        const target = e.target.closest('.tab-btn');
        if (!target) return;

        const tabName = target.dataset.tab;
        changeTab(tabName);
    }
    
    function handleAddUser() {
        // ... implementation for adding a user
    }
    
    // ... (All other event handlers)

    // --- Core Logic Functions ---
    function changeTab(tabName) {
        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(tab => tab.classList.add('hidden'));
        document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

        // Show selected tab
        const tabEl = document.getElementById(`${tabName}-tab`);
        if (tabEl) tabEl.classList.remove('hidden');

        const btn = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
        if (btn) btn.classList.add('active');

        // Logic for specific tabs
        if (tabName === 'data-analysis') {
            updateDataAnalysisCharts();
        }
        // ... other tab-specific logic
    }

    function updateAllCalculationsAndViews() {
        state.cachedStats = calculateAllPlayerStats(state.games);
        
        // Call all necessary update/render functions
        updateLeaderboard();
        // ... and so on
    }

    // --- Firebase Functions ---
    function setupListeners() {
        if (!state.currentUser) return;

        const usersCollectionRef = collection(state.db, `users`);
        onSnapshot(usersCollectionRef, (snapshot) => {
            state.users = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
            state.users.sort((a, b) => a.name.localeCompare(b.name, 'ja'));
            updateAllCalculationsAndViews();
        });

        const gamesCollectionRef = collection(state.db, `games`);
        const gamesQuery = query(gamesCollectionRef, orderBy("createdAt", "desc"));
        onSnapshot(gamesQuery, (snapshot) => {
            state.games = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
            updateAllCalculationsAndViews();
        });
    }

    // --- Initialization ---
    function bindEvents() {
        DOMElements.tabNavigation.addEventListener('click', handleTabClick);
        
        // Use event delegation for dynamically added content
        DOMElements.app.addEventListener('click', (e) => {
            if (e.target.matches('#add-user-btn')) {
                handleAddUser();
            }
            if (e.target.closest('.delete-user-btn')) {
                // handle delete user
            }
            // ... add all other event listeners here
        });
    }

    function init() {
        const app = initializeApp(CONSTANTS.FIREBASE_CONFIG);
        state.auth = getAuth(app);
        state.db = getFirestore(app);
        state.storage = getStorage(app);

        onAuthStateChanged(state.auth, async (user) => {
            if (user) {
                state.currentUser = user;
                DOMElements.authStatus.textContent = `System Online // User: ${user.isAnonymous ? 'Guest' : user.uid}`;
                await setupListeners();
            } else {
                try {
                    await signInAnonymously(state.auth);
                } catch (error) {
                    console.error("Authentication failed:", error);
                    DOMElements.authStatus.textContent = 'Authentication Failure';
                }
            }
        });
        
        // Initial render of all tab containers
        renderGameTab();
        renderLeaderboardTab();
        // ... render all other tabs
        
        bindEvents();
    }

    // --- Public API ---
    // Expose only necessary functions to the global scope if needed,
    // but with addEventListener, it's often not necessary.
    return {
        init: init
    };
})();

// --- Start the App ---
document.addEventListener('DOMContentLoaded', App.init);
