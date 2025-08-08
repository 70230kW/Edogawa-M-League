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
            '九蓮宝燈': ['国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '四槓子', '天和', '地和', '国士無双十三面待ち', '四暗刻単騎', '大四喜', '小四喜'],
            '純正九蓮宝燈': ['国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '四槓子', '天和', '地和', '国士無双十三面待ち', '四暗刻単騎', '大四喜', '小四喜'],
            '四槓子': ['国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '天和', '地和', '国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'],
            '四暗刻': ['国士無双', '九蓮宝燈', '四槓子', '国士無双十三面待ち', '純正九蓮宝燈'],
            '四暗刻単騎': ['国士無双', '九蓮宝燈', '四槓子', '国士無双十三面待ち', '純正九蓮宝燈'],
            '大三元': ['国士無双', '九蓮宝燈', '四槓子', '緑一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈'],
            '字一色': ['国士無双', '九蓮宝燈', '緑一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈'],
            '緑一色': ['国士無双', '九蓮宝燈', '大三元', '字一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈', '大四喜', '小四喜'],
            '清老頭': ['国士無双', '九蓮宝燈', '大三元', '字一色', '緑一色', '国士無双十三面待ち', '純正九蓮宝燈', '大四喜', '小四喜'],
            '大四喜': ['国士無双', '九蓮宝燈', '小四喜', '国士無双十三面待ち', '純正九蓮宝燈'],
            '小四喜': ['国士無双', '九蓮宝燈', '大四喜', '国士無双十三面待ち', '純正九蓮宝燈'],
        },
        FIREBASE_CONFIG: {
            apiKey: "AIzaSyBwWqWxRy5JlcQwbc5KAXRvH0swd0pOzSg",
            authDomain: "edogawa-m-league-summary.firebaseapp.com",
            projectId: "edogawa-m-league-summary",
            storageBucket: "edogawa-m-league-summary.appspot.com",
            messagingSenderId: "587593171009",
            appId: "1:587593171009:web:b48dd5b809f2d2ce8886c0",
            measurementId: "G-XMYXPG06QF"
        },
        TROPHY_DEFINITIONS: {
            bronze: [
                { id: 'first_game', name: '初陣', desc: '初めて対局に参加する', icon: 'fa-chess-pawn' },
                { id: 'first_top', name: '初トップ', desc: '初めてトップを取る', icon: 'fa-crown' },
            ],
            silver: [
                { id: 'yakuman', name: '神域の淵', desc: '初めて役満を和了する', icon: 'fa-dragon' },
            ],
            // ... (add all trophy definitions)
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
    };

    // --- Helper & Calculation Functions ---
    function getGameYears() {
        const years = new Set();
        state.games.forEach(game => {
            const dateStr = game.gameDate;
            if (dateStr) {
                const year = dateStr.substring(0, 4);
                if (!isNaN(parseInt(year))) years.add(year);
            } else if (game.createdAt && game.createdAt.seconds) {
                const year = new Date(game.createdAt.seconds * 1000).getFullYear().toString();
                years.add(year);
            }
        });
        return Array.from(years).sort((a, b) => parseInt(b) - parseInt(a));
    }

    function calculateAllPlayerStats(gamesToCalculate) {
        const stats = {};
        state.users.forEach(u => {
            stats[u.id] = { id: u.id, name: u.name, photoURL: u.photoURL, totalPoints: 0, gameCount: 0, ranks: [0, 0, 0, 0], bustedCount: 0, totalRawScore: 0, totalHanchans: 0, yakumanCount: 0, maxStreak: { rentai: 0, noTobi: 0, noLast: 0, top: 0, sameRank: 0 }, currentStreak: { rentai: 0, noTobi: 0, noLast: 0, top: 0, sameRank: 0 }, lastRank: null };
        });

        const sortedGames = [...gamesToCalculate].sort((a, b) => (a.createdAt?.seconds || 0) - (b.createdAt?.seconds || 0));

        sortedGames.forEach(game => {
            game.playerIds.forEach(pId => { if (stats[pId]) stats[pId].gameCount++; });
            Object.entries(game.totalPoints).forEach(([playerId, point]) => { if (stats[playerId]) stats[playerId].totalPoints += point; });

            game.scores.forEach(hanchan => {
                Object.entries(hanchan.rawScores).forEach(([playerId, rawScore]) => {
                    if (stats[playerId]) {
                        stats[playerId].totalRawScore += rawScore;
                        if (rawScore < 0) {
                            stats[playerId].bustedCount++;
                            stats[playerId].currentStreak.noTobi = 0;
                        } else {
                            stats[playerId].currentStreak.noTobi++;
                        }
                        stats[playerId].maxStreak.noTobi = Math.max(stats[playerId].maxStreak.noTobi, stats[playerId].currentStreak.noTobi);
                    }
                });
                
                const scoreGroups = {};
                Object.entries(hanchan.rawScores).forEach(([pId, score]) => {
                    if (!scoreGroups[score]) scoreGroups[score] = [];
                    scoreGroups[score].push(pId);
                });
                const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
                
                let rankCursor = 0;
                sortedScores.forEach(score => {
                    const playersInGroup = scoreGroups[score];
                    playersInGroup.forEach(pId => {
                        if (stats[pId]) {
                            const currentRank = rankCursor;
                            stats[pId].ranks[currentRank]++;

                            if (currentRank <= 1) { stats[pId].currentStreak.rentai++; } else { stats[pId].currentStreak.rentai = 0; }
                            if (currentRank === 0) { stats[pId].currentStreak.top++; } else { stats[pId].currentStreak.top = 0; }
                            if (currentRank === 3) { stats[pId].currentStreak.noLast = 0; } else { stats[pId].currentStreak.noLast++; }
                            if(stats[pId].lastRank === currentRank) { stats[pId].currentStreak.sameRank++; } else { stats[pId].currentStreak.sameRank = 1; }

                            stats[pId].maxStreak.rentai = Math.max(stats[pId].maxStreak.rentai, stats[pId].currentStreak.rentai);
                            stats[pId].maxStreak.top = Math.max(stats[pId].maxStreak.top, stats[pId].currentStreak.top);
                            stats[pId].maxStreak.noLast = Math.max(stats[pId].maxStreak.noLast, stats[pId].currentStreak.noLast);
                            stats[pId].maxStreak.sameRank = Math.max(stats[pId].maxStreak.sameRank, stats[pId].currentStreak.sameRank);
                            stats[pId].lastRank = currentRank;
                        }
                    });
                    rankCursor += playersInGroup.length;
                });

                Object.keys(hanchan.rawScores).forEach(pId => {
                    if(stats[pId]) stats[pId].totalHanchans++;
                });

                if (hanchan.yakumanEvents) {
                    hanchan.yakumanEvents.forEach(event => {
                        if (stats[event.playerId]) {
                            stats[event.playerId].yakumanCount += event.yakumans.length;
                        }
                    });
                }
            });
        });

        Object.values(stats).forEach(u => {
            if (u.totalHanchans > 0) {
                u.avgRank = u.ranks.reduce((sum, count, i) => sum + count * (i + 1), 0) / u.totalHanchans;
                u.topRate = (u.ranks[0] / u.totalHanchans) * 100;
                u.rentaiRate = ((u.ranks[0] + u.ranks[1]) / u.totalHanchans) * 100;
                u.lastRate = (u.ranks[3] / u.totalHanchans) * 100;
                u.bustedRate = (u.bustedCount / u.totalHanchans) * 100;
                u.avgRawScore = Math.round((u.totalRawScore / u.totalHanchans) / 100) * 100;
            } else {
                u.avgRank = 0; u.topRate = 0; u.rentaiRate = 0; u.lastRate = 0; u.bustedRate = 0; u.avgRawScore = 0;
            }
        });

        return stats;
    }

    // ... (All other calculation functions)
    
    // --- UI Rendering Functions ---
    function renderAllTabs() {
        renderGameTab();
        renderLeaderboardTab();
        renderTrophyTab();
        renderDataAnalysisTab();
        renderPersonalStatsTab();
        renderUserManagementTab();
        renderDetailedHistoryTabContainers();
        renderHeadToHeadTab();
        renderHistoryTab();
    }
    
    function getPlayerPhotoHtml(playerId, sizeClass = 'w-12 h-12') {
        const user = state.users.find(u => u.id === playerId);
        const fontSize = parseInt(sizeClass.match(/w-(\d+)/)[1]) / 2.5;
        if (user && user.photoURL) {
            return `<img src="${user.photoURL}" class="${sizeClass} rounded-full object-cover bg-gray-800" alt="${user.name}" onerror="this.onerror=null;this.src='https://placehold.co/100x100/010409/8b949e?text=?';">`;
        }
        return `<div class="${sizeClass} rounded-full bg-gray-700 flex items-center justify-center">
                    <i class="fas fa-user text-gray-500" style="font-size: ${fontSize}px;"></i>
                </div>`;
    }

    // ... (All other render functions from original code, adapted to use state and DOMElements)
    function renderGameTab() {
        // This function now just creates the static HTML structure.
        // Dynamic parts are handled by other functions like renderPlayerSelection.
        const container = document.getElementById('game-tab');
        if (!container) return;
        container.innerHTML = `
            <div id="step1-player-selection" class="cyber-card p-4 sm:p-6">
                <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 1: 雀士選択</h2>
                <div id="player-list-for-selection" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4"></div>
                <div class="flex justify-end">
                    <button id="to-step2-btn" class="cyber-btn px-6 py-2 rounded-lg" disabled>進む <i class="fas fa-arrow-right ml-2"></i></button>
                </div>
            </div>
            <div id="step2-rule-settings" class="cyber-card p-4 sm:p-6 hidden">
                <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 2: ルール選択</h2>
                <div class="space-y-4">
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div><label class="block text-sm font-medium text-gray-400">基準点</label><input type="number" id="base-point" value="25000" class="mt-1 block w-full rounded-md shadow-sm sm:text-sm"></div>
                        <div><label class="block text-sm font-medium text-gray-400">返し点</label><input type="number" id="return-point" value="30000" class="mt-1 block w-full rounded-md shadow-sm sm:text-sm"></div>
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-400">順位ウマ</label>
                        <div class="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-1">
                            <input type="number" id="uma-1" placeholder="1位" value="30" class="w-full rounded-md shadow-sm sm:text-sm">
                            <input type="number" id="uma-2" placeholder="2位" value="10" class="w-full rounded-md shadow-sm sm:text-sm">
                            <input type="number" id="uma-3" placeholder="3位" value="-10" class="w-full rounded-md shadow-sm sm:text-sm">
                            <input type="number" id="uma-4" placeholder="4位" value="-30" class="w-full rounded-md shadow-sm sm:text-sm">
                        </div>
                    </div>
                    <div class="flex flex-wrap items-center justify-between gap-4">
                        <button id="m-league-rules-btn" class="cyber-btn cyber-btn-green px-4 py-2 rounded-lg"><i class="fas fa-star mr-2"></i>Mリーグルール</button>
                        <div class="text-right"><p class="text-sm text-gray-400">オカ: <span id="oka-display" class="font-bold text-white">20</span></p></div>
                    </div>
                </div>
                <div class="flex justify-between mt-6">
                    <button id="back-to-step1-btn" class="cyber-btn px-6 py-2 rounded-lg"><i class="fas fa-arrow-left mr-2"></i>戻る</button>
                    <button id="to-step3-btn" class="cyber-btn px-6 py-2 rounded-lg">進む <i class="fas fa-arrow-right ml-2"></i></button>
                </div>
            </div>
            <div id="step3-score-input" class="cyber-card p-4 sm:p-6 hidden">
                <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 3: 素点入力</h2>
                <div class="mb-4">
                    <label for="game-date" class="block text-sm font-medium text-gray-400 mb-1">対局日</label>
                    <div class="flex gap-2">
                        <input type="text" id="game-date" class="flex-grow rounded-md shadow-sm sm:text-sm" placeholder="yyyy/m/d(aaa)">
                        <button id="today-btn" class="cyber-btn px-4 py-2 rounded-lg">今日</button>
                    </div>
                </div>
                <div id="score-display-area" class="space-y-4"></div>
                <div class="flex flex-col sm:flex-row justify-between items-center mt-4 flex-wrap gap-4">
                    <div class="flex gap-2 w-full sm:w-auto">
                        <button id="add-hanchan-btn" class="cyber-btn px-4 py-2 rounded-lg w-full"><i class="fas fa-plus mr-2"></i>半荘追加</button>
                        <button id="save-partial-btn" class="cyber-btn cyber-btn-green px-4 py-2 rounded-lg w-full"><i class="fas fa-floppy-disk mr-2"></i>途中保存</button>
                    </div>
                    <button id="show-pt-status-btn" class="cyber-btn px-4 py-2 rounded-lg order-first sm:order-none w-full sm:w-auto"><i class="fas fa-calculator mr-2"></i>現在のPT状況</button>
                    <div class="flex gap-2 w-full sm:w-auto">
                        <button id="back-to-step2-btn" class="cyber-btn px-6 py-2 rounded-lg w-full"><i class="fas fa-arrow-left mr-2"></i>戻る</button>
                        <button id="save-game-btn" class="cyber-btn cyber-btn-yellow px-6 py-2 rounded-lg w-full"><i class="fas fa-save mr-2"></i>Pt変換して保存</button>
                    </div>
                </div>
            </div>
        `;
        renderPlayerSelection();
    }
    // ... (All other render functions must be fully implemented here)

    // --- Event Handlers ---
    function handleTabClick(e) {
        const target = e.target.closest('.tab-btn');
        if (!target) return;
        changeTab(target.dataset.tab);
    }
    
    // ... (All other event handlers)

    // --- Core Logic Functions ---
    function changeTab(tabName) {
        document.querySelectorAll('.tab-content').forEach(tab => tab.classList.add('hidden'));
        document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

        const tabEl = document.getElementById(`${tabName}-tab`);
        if (tabEl) tabEl.classList.remove('hidden');

        const btn = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
        if (btn) btn.classList.add('active');

        if (tabName === 'data-analysis') {
            // updateDataAnalysisCharts();
        }
        // ... other tab-specific logic
    }

    function updateAllCalculationsAndViews() {
        state.cachedStats = calculateAllPlayerStats(state.games);
        
        const activeTab = document.querySelector('.tab-btn.active')?.dataset.tab || 'game';

        // Update data for all tabs in the background
        // updateLeaderboard();
        // updateTrophyPage();
        // updateHistoryTabFilters();
        // updateHistoryList();
        // renderDetailedHistoryTables();
        // renderUserManagementList();
        // renderPersonalStatsTab();
        // updateHeadToHeadDropdowns();

        // Refresh view only for the active tab to improve performance
        switch (activeTab) {
            case 'leaderboard':
                // updateLeaderboard();
                break;
            // ... add cases for all other tabs
        }
    }

    // --- Firebase Functions ---
    function setupListeners() {
        if (!state.currentUser) return;

        const usersCollectionRef = collection(state.db, `users`);
        onSnapshot(query(usersCollectionRef, orderBy("name", "asc")), (snapshot) => {
            state.users = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
            // Instead of calling the heavy update function right away,
            // we can call a lighter version that only updates what's necessary
            // For simplicity now, we'll keep the full update.
            updateAllCalculationsAndViews();
        });

        const gamesCollectionRef = collection(state.db, `games`);
        onSnapshot(query(gamesCollectionRef, orderBy("createdAt", "desc")), (snapshot) => {
            state.games = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
            updateAllCalculationsAndViews();
        });
    }

    // --- Initialization ---
    function bindEvents() {
        DOMElements.tabNavigation.addEventListener('click', handleTabClick);
        
        // Event delegation for the whole app
        DOMElements.app.addEventListener('click', (e) => {
            const target = e.target;
            const targetId = target.id;
            const targetClosest = (selector) => target.closest(selector);

            // Game Tab Buttons
            if (targetId === 'to-step2-btn') moveToStep2();
            if (targetId === 'to-step3-btn') moveToStep3();
            if (targetId === 'back-to-step1-btn') backToStep1();
            if (targetId === 'back-to-step2-btn') backToStep2();
            if (targetId === 'm-league-rules-btn') setMLeagueRules();
            if (targetId === 'today-btn') setTodayDate();
            if (targetId === 'add-hanchan-btn') addHanchan();
            if (targetId === 'save-partial-btn') savePartialData();
            if (targetId === 'show-pt-status-btn') showCurrentPtStatus();
            if (targetId === 'save-game-btn') calculateAndSave();
            
            // User Management
            if (targetId === 'add-user-btn') addUser();
            
            // ... (Add all other click handlers here using their IDs)
        });

        DOMElements.app.addEventListener('change', (e) => {
            const target = e.target;
            // Player selection checkboxes
            if (target.matches('.player-checkbox')) {
                togglePlayerSelection(target);
            }
            // ... (Add all change handlers for select dropdowns etc.)
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
                renderAllTabs(); // Render tab structure
                bindEvents(); // Bind events to the static structure
                await setupListeners(); // Start listening for data
            } else {
                try {
                    await signInAnonymously(state.auth);
                } catch (error) {
                    console.error("Authentication failed:", error);
                    DOMElements.authStatus.textContent = 'Authentication Failure';
                }
            }
        });
    }

    return {
        init: init
    };
})();

// --- Start the App ---
document.addEventListener('DOMContentLoaded', App.init);
