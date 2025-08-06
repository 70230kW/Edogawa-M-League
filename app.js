import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInAnonymously, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, collection, doc, addDoc, getDocs, onSnapshot, deleteDoc, query, where, writeBatch, orderBy, updateDoc } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getStorage, ref, uploadBytes, getDownloadURL } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-storage.js";

// --- App State ---
let db, auth, storage;
let currentUser = null;
let users = [];
let games = [];
let selectedPlayers = []; // {id, name, photoURL}
let hanchanScores = [];   // Holds scores for each hanchan
let activeInputId = null; // Holds the ID of the active score input in the modal
let editingGameId = null; // ID of the game being edited

// --- Performance Refactor: Cached Stats ---
let cachedStats = {};
let playerTrophies = {}; // { playerId: { trophyId: true, ... } }

// Chart instances
let playerRadarChart = null;
let pointHistoryChart = null;
let playerBarChart = null;
let personalRankChart = null;
let personalPointHistoryChart = null;

// --- Constants ---
const YAKUMAN_LIST = [
    '国士無双', '四暗刻', '大三元', '緑一色', '字一色', '清老頭', '九蓮宝燈', '四槓子', '天和', '地和',
    '国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜', '小四喜'
];
const PENALTY_REASONS = {
    chombo: ['誤ロン・誤ツモ', 'ノーテンリーチ', 'その他'],
    agariHouki: ['多牌・少牌', '喰い替え', 'その他']
};
const YAKUMAN_INCOMPATIBILITY = {
    '天和': YAKUMAN_LIST.filter(y => y !== '天和'),
    '地和': YAKUMAN_LIST.filter(y => y !== '地和'),
    '国士無双': YAKUMAN_LIST.filter(y => !['国士無双', '国士無双十三面待ち'].includes(y)),
    '国士無双十三面待ち': YAKUMAN_LIST.filter(y => !['国士無双', '国士無双十三面待ち'].includes(y)),
    '九蓮宝燈': YAKUMAN_LIST.filter(y => !['九蓮宝燈', '純正九蓮宝燈'].includes(y)),
    '純正九蓮宝燈': YAKUMAN_LIST.filter(y => !['九蓮宝燈', '純正九蓮宝燈'].includes(y)),
    '四槓子': YAKUMAN_LIST.filter(y => y !== '四槓子'),
    '四暗刻': ['国士無双', '九蓮宝燈', '四槓子', '国士無双十三面待ち', '純正九蓮宝燈'],
    '四暗刻単騎': ['国士無双', '九蓮宝燈', '四槓子', '国士無双十三面待ち', '純正九蓮宝燈'],
    '大三元': ['国士無双', '九蓮宝燈', '四槓子', '緑一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈'],
    '字一色': ['国士無双', '九蓮宝燈', '緑一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈'],
    '緑一色': ['国士無双', '九蓮宝燈', '大三元', '字一色', '清老頭', '国士無双十三面待ち', '純正九蓮宝燈', '大四喜', '小四喜'],
    '清老頭': ['国士無双', '九蓮宝燈', '大三元', '字一色', '緑一色', '国士無双十三面待ち', '純正九蓮宝燈', '大四喜', '小四喜'],
    '大四喜': ['国士無双', '九蓮宝燈', '小四喜', '国士無双十三面待ち', '純正九蓮宝燈'],
    '小四喜': ['国士無双', '九蓮宝燈', '大四喜', '国士無双十三面待ち', '純正九蓮宝燈'],
};

// --- Firebase Config ---
const firebaseConfig = {
    apiKey: "AIzaSyBwWqWxRy5JlcQwbc5KAXRvH0swd0pOzSg",
    authDomain: "edogawa-m-league-summary.firebaseapp.com",
    projectId: "edogawa-m-league-summary",
    storageBucket: "edogawa-m-league-summary.appspot.com",
    messagingSenderId: "587593171009",
    appId: "1:587593171009:web:b48dd5b809f2d2ce8886c0",
    measurementId: "G-XMYXPG06QF"
};

// --- Helper/Calculation Functions ---
function getGameYears() {
    const years = new Set();
    games.forEach(game => {
        const dateStr = game.gameDate;
        if (dateStr) {
            const year = dateStr.substring(0, 4);
            if (!isNaN(year)) {
                years.add(year);
            }
        } else if (game.createdAt && game.createdAt.seconds) {
            const year = new Date(game.createdAt.seconds * 1000).getFullYear().toString();
            years.add(year);
        }
    });
    return Array.from(years).sort((a, b) => b - a);
}

/**
 * Calculates all statistics for all players based on a given set of games.
 * This is a computationally intensive function and should be called sparingly.
 * @param {Array} gamesToCalculate - The array of game objects to process.
 * @returns {Object} An object where keys are player IDs and values are their calculated stats.
 */
function calculateAllPlayerStats(gamesToCalculate) {
    const stats = {};
    users.forEach(u => {
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

                        // Streak logic
                        if (currentRank <= 1) { // 1st or 2nd
                            stats[pId].currentStreak.rentai++;
                        } else {
                            stats[pId].currentStreak.rentai = 0;
                        }
                        if (currentRank === 0) { // 1st
                            stats[pId].currentStreak.top++;
                        } else {
                            stats[pId].currentStreak.top = 0;
                        }
                        if (currentRank === 3) { // 4th
                            stats[pId].currentStreak.noLast = 0;
                        } else {
                            stats[pId].currentStreak.noLast++;
                        }
                        if(stats[pId].lastRank === currentRank) {
                            stats[pId].currentStreak.sameRank++;
                        } else {
                            stats[pId].currentStreak.sameRank = 1;
                        }

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

    // Calculate derived stats
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

function getPlayerPointHistory(playerId, fullTimeline) {
    let cumulativePoints = 0;
    const playerGamesByDate = {};
    [...games]
        .filter(g => g.playerIds.includes(playerId))
        .forEach(game => {
            const date = game.gameDate.split('(')[0];
            if (!playerGamesByDate[date]) {
                playerGamesByDate[date] = 0;
            }
            playerGamesByDate[date] += game.totalPoints[playerId];
        });

    const history = [];
    fullTimeline.forEach(date => {
        if (playerGamesByDate[date]) {
            cumulativePoints += playerGamesByDate[date];
        }
        history.push(cumulativePoints);
    });
    return history;
}

// --- Initialization ---
function initializeAppAndAuth() {
    const app = initializeApp(firebaseConfig);
    auth = getAuth(app);
    db = getFirestore(app);
    storage = getStorage(app);

    onAuthStateChanged(auth, async (user) => {
        if (user) {
            currentUser = user;
            document.getElementById('auth-status').textContent = `System Online // User: ${user.isAnonymous ? 'Guest' : user.uid}`;
            await setupListeners();
        } else {
            try {
                await signInAnonymously(auth);
            } catch (error) {
                console.error("Authentication failed:", error);
                document.getElementById('auth-status').textContent = 'Authentication Failure';
            }
        }
    });
    
    // Initial render of all tab containers
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

/**
 * Performance Refactor: Master update function.
 */
function updateAllCalculationsAndViews() {
    cachedStats = calculateAllPlayerStats(games);
    
    updateLeaderboard();
    updateTrophyPage();
    updateHistoryTabFilters();
    updateHistoryList();
    renderDetailedHistoryTables();
    renderUserManagementList();
    renderPersonalStatsTab();
    updateHeadToHeadDropdowns();
    
    const activeTab = document.querySelector('.tab-btn.active').getAttribute('onclick').match(/'([^']+)'/)[1];
    
    if (activeTab === 'data-analysis') {
        updateDataAnalysisCharts();
    } else if (activeTab === 'personal-stats') {
        const playerId = document.getElementById('personal-stats-player-select')?.value;
        if (playerId) {
            displayPlayerStats(playerId);
        }
    } else if (activeTab === 'game') {
        if (document.getElementById('step2-rule-settings')?.classList.contains('hidden')) {
            renderPlayerSelection();
        }
        if(!document.getElementById('step3-score-input').classList.contains('hidden')) {
            renderScoreDisplay();
        }
    } else if (activeTab === 'head-to-head') {
         displayHeadToHeadStats();
    }
}

async function setupListeners() {
    if (!currentUser) return;

    const usersCollectionRef = collection(db, `users`);
    onSnapshot(usersCollectionRef, (snapshot) => {
        users = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        users.sort((a, b) => a.name.localeCompare(b.name, 'ja'));
        
        selectedPlayers = selectedPlayers.map(sp => {
            const updatedUser = users.find(u => u.id === sp.id);
            return updatedUser ? { id: updatedUser.id, name: updatedUser.name, photoURL: updatedUser.photoURL } : sp;
        });
        
        updateAllCalculationsAndViews();
    });

    const gamesCollectionRef = collection(db, `games`);
    const gamesQuery = query(gamesCollectionRef, orderBy("createdAt", "desc"));
    onSnapshot(gamesQuery, (snapshot) => {
        games = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        updateAllCalculationsAndViews();
    });
}

// --- UI Rendering ---

function getPlayerPhotoHtml(playerId, sizeClass = 'w-12 h-12') {
    const user = users.find(u => u.id === playerId);
    const fontSize = parseInt(sizeClass.match(/w-(\d+)/)[1]) / 2.5;
    if (user && user.photoURL) {
        return `<img src="${user.photoURL}" class="${sizeClass} rounded-full object-cover bg-gray-800" alt="${user.name}" onerror="this.onerror=null;this.src='https://placehold.co/100x100/010409/8b949e?text=?';">`;
    }
    return `<div class="${sizeClass} rounded-full bg-gray-700 flex items-center justify-center">
                <i class="fas fa-user text-gray-500" style="font-size: ${fontSize}px;"></i>
            </div>`;
}

window.changeTab = (tabName) => {
    ['game', 'leaderboard', 'trophy', 'data-analysis', 'personal-stats', 'history', 'head-to-head', 'history-raw', 'history-pt', 'users'].forEach(tab => {
        const tabEl = document.getElementById(`${tab}-tab`);
        if(tabEl) tabEl.classList.add('hidden');
        
        const btn = document.querySelector(`.tab-btn[onclick="changeTab('${tab}')"]`);
        if(btn) btn.classList.remove('active');
    });
    const tabEl = document.getElementById(`${tabName}-tab`);
    if(tabEl) tabEl.classList.remove('hidden');

    const btn = document.querySelector(`.tab-btn[onclick="changeTab('${tabName}')"]`);
    if(btn) btn.classList.add('active');

    if (tabName === 'data-analysis') {
        updateDataAnalysisCharts();
    } else if (tabName === 'game' && editingGameId === null) {
        loadSavedGameData();
    } else if (tabName === 'head-to-head') {
        displayHeadToHeadStats();
    } else if (tabName === 'trophy') {
        updateTrophyPage();
    }
};

function renderGameTab() {
    const container = document.getElementById('game-tab');
    container.innerHTML = `
        <div id="step1-player-selection" class="cyber-card p-4 sm:p-6">
            <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 1: 雀士選択</h2>
            <div id="player-list-for-selection" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4"></div>
            <div class="flex justify-end">
                <button id="to-step2-btn" onclick="moveToStep2()" class="cyber-btn px-6 py-2 rounded-lg" disabled>進む <i class="fas fa-arrow-right ml-2"></i></button>
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
                    <button onclick="setMLeagueRules()" class="cyber-btn cyber-btn-green px-4 py-2 rounded-lg"><i class="fas fa-star mr-2"></i>Mリーグルール</button>
                    <div class="text-right"><p class="text-sm text-gray-400">オカ: <span id="oka-display" class="font-bold text-white">20</span></p></div>
                </div>
            </div>
            <div class="flex justify-between mt-6">
                <button onclick="backToStep1()" class="cyber-btn px-6 py-2 rounded-lg"><i class="fas fa-arrow-left mr-2"></i>戻る</button>
                <button onclick="moveToStep3()" class="cyber-btn px-6 py-2 rounded-lg">進む <i class="fas fa-arrow-right ml-2"></i></button>
            </div>
        </div>
        <div id="step3-score-input" class="cyber-card p-4 sm:p-6 hidden">
             <h2 class="cyber-header text-xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">STEP 3: 素点入力</h2>
             <div class="mb-4">
                 <label for="game-date" class="block text-sm font-medium text-gray-400 mb-1">対局日</label>
                 <div class="flex gap-2">
                     <input type="text" id="game-date" class="flex-grow rounded-md shadow-sm sm:text-sm" placeholder="yyyy/m/d(aaa)">
                     <button onclick="setTodayDate()" class="cyber-btn px-4 py-2 rounded-lg">今日</button>
                 </div>
             </div>
             <div id="score-display-area" class="space-y-4"></div>
             <div class="flex flex-col sm:flex-row justify-between items-center mt-4 flex-wrap gap-4">
                 <div class="flex gap-2 w-full sm:w-auto">
                     <button onclick="addHanchan()" class="cyber-btn px-4 py-2 rounded-lg w-full"><i class="fas fa-plus mr-2"></i>半荘追加</button>
                     <button onclick="savePartialData()" class="cyber-btn cyber-btn-green px-4 py-2 rounded-lg w-full"><i class="fas fa-floppy-disk mr-2"></i>途中保存</button>
                 </div>
                 <button onclick="showCurrentPtStatus()" class="cyber-btn px-4 py-2 rounded-lg order-first sm:order-none w-full sm:w-auto"><i class="fas fa-calculator mr-2"></i>現在のPT状況</button>
                 <div class="flex gap-2 w-full sm:w-auto">
                     <button onclick="backToStep2()" class="cyber-btn px-6 py-2 rounded-lg w-full"><i class="fas fa-arrow-left mr-2"></i>戻る</button>
                     <button id="save-game-btn" onclick="calculateAndSave()" class="cyber-btn cyber-btn-yellow px-6 py-2 rounded-lg w-full"><i class="fas fa-save mr-2"></i>Pt変換して保存</button>
                 </div>
             </div>
        </div>
    `;
    renderPlayerSelection();
    setupRuleEventListeners();
}

function renderLeaderboardTab() {
    const container = document.getElementById('leaderboard-tab');
    if (!container) return;
    const yearOptions = getGameYears().map(year => `<option value="${year}">${year}年</option>`).join('');

    container.innerHTML = `
        <div class="flex flex-col sm:flex-row justify-between items-center mb-4 gap-4">
            <h2 class="cyber-header text-2xl font-bold text-blue-400">順位表</h2>
            <div class="flex items-center gap-2">
                <label for="leaderboard-period-select" class="text-sm whitespace-nowrap">期間:</label>
                <select id="leaderboard-period-select" onchange="updateAllCalculationsAndViews()" class="rounded-md p-1">
                    <option value="all">全期間</option>
                    ${yearOptions}
                </select>
            </div>
        </div>

        <div id="leaderboard-cards-container" class="space-y-4 md:hidden"></div>

        <div class="overflow-x-auto hidden md:block">
            <table class="min-w-full divide-y divide-gray-700 leaderboard-table">
                <thead class="bg-gray-900 text-xs md:text-sm font-medium text-gray-400 uppercase tracking-wider">
                    <tr>
                        <th class="px-2 py-3 text-right sticky-col-1 w-14 whitespace-nowrap">順位</th>
                        <th class="px-2 py-3 text-left sticky-col-2 whitespace-nowrap">雀士</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">合計Pt</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">半荘数</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">平均着順</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">トップ率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">2着率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">3着率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">ラス率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">連対率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">トビ率</th>
                        <th class="px-2 md:px-4 py-3 text-right whitespace-nowrap">平均素点</th>
                    </tr>
                </thead>
                <tbody id="leaderboard-body" class="divide-y divide-gray-700"></tbody>
            </table>
        </div>
    `;
}

function renderUserManagementTab() {
    const container = document.getElementById('users-tab');
    container.innerHTML = `
        <h2 class="cyber-header text-2xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">雀士管理</h2>
        <div class="flex flex-col sm:flex-row gap-4 mb-4">
            <input type="text" id="new-user-name" class="flex-grow rounded-md shadow-sm sm:text-sm" placeholder="新しい雀士名">
            <button onclick="addUser()" class="cyber-btn px-6 py-2 rounded-lg whitespace-nowrap"><i class="fas fa-user-plus mr-2"></i>追加</button>
        </div>
        <div id="user-list-management" class="space-y-2"></div>
    `;
}

function renderPersonalStatsTab() {
    const container = document.getElementById('personal-stats-tab');
    const options = users.map(u => `<option value="${u.id}">${u.name}</option>`).join('');
    container.innerHTML = `
        <div class="flex flex-col sm:flex-row flex-wrap justify-between items-center mb-4 gap-4">
            <h2 class="cyber-header text-2xl font-bold pb-2 text-blue-400">個人成績</h2>
            <div class="flex items-center gap-2">
                <label for="personal-stats-player-select" class="text-sm text-gray-400">雀士:</label>
                <select id="personal-stats-player-select" onchange="displayPlayerStats(this.value)" class="rounded-md p-1">
                    <option value="">選択してください</option>
                    ${options}
                </select>
            </div>
        </div>
        <div id="personal-stats-content" class="space-y-6">
            <p class="text-gray-500">雀士を選択して成績を表示します。</p>
        </div>
    `;
}

function renderDetailedHistoryTabContainers() {
    const rawContainer = document.getElementById('history-raw-tab');
    const ptContainer = document.getElementById('history-pt-tab');
    const filterHtml = (prefix) => `
        <div class="flex flex-col sm:flex-row gap-4 mb-4 p-4 bg-gray-900 rounded-lg">
            <div class="flex-1">
                <label for="history-${prefix}-year-filter" class="block text-sm font-medium text-gray-400">年</label>
                <select id="history-${prefix}-year-filter" onchange="renderDetailedHistoryTables()" class="mt-1 block w-full rounded-md"></select>
            </div>
            <div class="flex-1">
                <label for="history-${prefix}-month-filter" class="block text-sm font-medium text-gray-400">月</label>
                <select id="history-${prefix}-month-filter" onchange="renderDetailedHistoryTables()" class="mt-1 block w-full rounded-md"></select>
            </div>
            <div class="flex-1">
                <label for="history-${prefix}-player-filter" class="block text-sm font-medium text-gray-400">雀士</label>
                <select id="history-${prefix}-player-filter" onchange="renderDetailedHistoryTables()" class="mt-1 block w-full rounded-md"></select>
            </div>
        </div>
    `;
    rawContainer.innerHTML = `<h2 class="cyber-header text-2xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">詳細履歴 (素点)</h2>${filterHtml('raw')}<div id="history-raw-list" class="overflow-x-auto"></div>`;
    ptContainer.innerHTML = `<h2 class="cyber-header text-2xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">詳細履歴 (PT)</h2>${filterHtml('pt')}<div id="history-pt-list" class="overflow-x-auto"></div>`;
}

function renderDataAnalysisTab() {
    const container = document.getElementById('data-analysis-tab');
    container.innerHTML = `
        <div class="space-y-6">
            <h2 class="cyber-header text-2xl font-bold text-blue-400 border-b border-gray-700 pb-2">データ分析ダッシュボード</h2>
            
            <div id="stat-cards-container" class="grid grid-cols-2 md:grid-cols-3 gap-4 text-center">
                </div>
            
            <div id="top-3-container" class="cyber-card p-4 sm:p-6"></div>

            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div class="cyber-card p-4 sm:p-6 min-h-[300px] md:min-h-[400px]">
                    <h3 class="cyber-header text-xl font-bold mb-4 text-center">雀士スタイル分析</h3>
                    <div id="radar-player-select" class="flex flex-wrap justify-center gap-x-4 gap-y-2 mb-2 text-sm"></div>
                    <div class="relative h-64 md:h-80"><canvas id="player-radar-chart"></canvas></div>
                </div>
                <div class="cyber-card p-4 sm:p-6 min-h-[300px] md:min-h-[400px]">
                    <h3 class="cyber-header text-xl font-bold mb-4 text-center">ポイント推移</h3>
                    <div class="relative h-72 md:h-96"><canvas id="point-history-chart"></canvas></div>
                </div>
                <div class="cyber-card p-4 sm:p-6 lg:col-span-2">
                     <div class="flex flex-col sm:flex-row justify-between items-center mb-4 gap-2">
                         <h3 class="cyber-header text-xl font-bold text-center">雀士スタッツ比較</h3>
                         <select id="bar-chart-metric-select" onchange="updateDataAnalysisCharts()" class="p-2 rounded-md text-sm"></select>
                     </div>
                    <div class="relative h-80"><canvas id="player-bar-chart"></canvas></div>
                </div>
            </div>
        </div>
    `;
}

function renderHeadToHeadTab() {
    const container = document.getElementById('head-to-head-tab');
    container.innerHTML = `
        <h2 class="cyber-header text-2xl font-bold text-blue-400 border-b border-gray-700 pb-2 mb-4">直接対決</h2>
        <div class="flex flex-col sm:flex-row items-center justify-center gap-4 mb-6">
            <select id="h2h-player1" class="p-2 rounded-md" onchange="displayHeadToHeadStats()"></select>
            <span class="font-bold text-xl">VS</span>
            <select id="h2h-player2" class="p-2 rounded-md" onchange="displayHeadToHeadStats()"></select>
        </div>
        <div id="h2h-results" class="text-center">
            <p class="text-gray-500">比較したい雀士を2名選択してください。</p>
        </div>
    `;
}

// --- Component Rendering ---

function renderPlayerSelection() {
    const container = document.getElementById('player-list-for-selection');
    if (!container) return;
    container.innerHTML = '';
    users.forEach(user => {
        const isSelected = selectedPlayers.some(p => p.id === user.id);
        const isDisabled = !isSelected && selectedPlayers.length >= 4;
        const photoHtml = getPlayerPhotoHtml(user.id, 'w-16 h-16');
        const div = document.createElement('div');
        div.innerHTML = `
            <input type="checkbox" id="player-${user.id}" class="player-checkbox hidden" value="${user.id}" name="${user.name}" onchange="togglePlayerSelection(this)" ${isSelected ? 'checked' : ''} ${isDisabled ? 'disabled' : ''}>
            <label for="player-${user.id}" class="block text-center border-2 border-gray-600 rounded-lg p-3 cursor-pointer transition-colors duration-200 hover:border-blue-500">
                <div class="w-16 h-16 mx-auto mb-2">${photoHtml}</div>
                <span>${user.name}</span>
            </label>
        `;
        container.appendChild(div);
    });
}

function renderUserManagementList() {
    const container = document.getElementById('user-list-management');
    if (!container) return;
    container.innerHTML = users.length === 0 
        ? `<p class="text-gray-500">登録されている雀士がいません。</p>`
        : users.map(user => {
            const photoHtml = getPlayerPhotoHtml(user.id, 'w-12 h-12');
            return `
            <div class="flex items-center gap-4 bg-gray-900 p-2 rounded-lg">
                <div class="relative flex-shrink-0">
                    <label for="photo-upload-${user.id}" class="cursor-pointer">
                        ${photoHtml}
                    </label>
                    <input type="file" id="photo-upload-${user.id}" class="hidden" accept="image/*" onchange="handlePhotoUpload('${user.id}', this)">
                </div>
                <div class="flex-grow">
                    <input type="text" id="user-name-input-${user.id}" value="${user.name}" data-original-name="${user.name}" class="w-full bg-transparent rounded-md p-1 -m-1 focus:bg-gray-800 focus:ring-1 focus:ring-blue-500 outline-none" readonly>
                </div>
                <div class="flex items-center gap-1 flex-shrink-0">
                    <button id="edit-user-btn-${user.id}" onclick="toggleEditUser('${user.id}')" class="cyber-btn text-sm px-3 py-1 rounded-md"><i class="fas fa-edit"></i></button>
                    <button onclick="confirmDeleteUser('${user.id}')" class="cyber-btn-red text-sm px-3 py-1 rounded-md"><i class="fas fa-trash-alt"></i></button>
                </div>
            </div>
        `}).join('');
}

window.updateLeaderboard = () => {
    const periodSelect = document.getElementById('leaderboard-period-select');
    if (!periodSelect) return;
    
    const currentPeriod = periodSelect.value;
    const yearOptions = getGameYears().map(year => `<option value="${year}" ${currentPeriod === year ? 'selected' : ''}>${year}年</option>`).join('');
    periodSelect.innerHTML = `<option value="all" ${currentPeriod === 'all' ? 'selected' : ''}>全期間</option>${yearOptions}`;
    
    const period = periodSelect.value;

    const filteredGames = games.filter(game => {
        if (period === 'all') return true;
        const gameDate = game.gameDate || new Date(game.createdAt.seconds * 1000).getFullYear().toString();
        const gameYear = gameDate.substring(0, 4);
        return gameYear === period;
    });

    const statsForPeriod = calculateAllPlayerStats(filteredGames);
    const rankedUsers = Object.values(statsForPeriod).filter(u => u.totalHanchans > 0);
    
    const leaderboardBody = document.getElementById('leaderboard-body');
    const cardsContainer = document.getElementById('leaderboard-cards-container');

    if (rankedUsers.length === 0) {
        if(leaderboardBody) leaderboardBody.innerHTML = `<tr><td colspan="12" class="text-center py-4 text-gray-500">NO DATA</td></tr>`;
        if(cardsContainer) cardsContainer.innerHTML = `<p class="text-center py-4 text-gray-500">NO DATA</p>`;
        return;
    }

    const minMax = {};
    const statFields = {
        avgRank: 'lower', thirdRate: 'lower', lastRate: 'lower', bustedRate: 'lower',
        topRate: 'higher', secondRate: 'higher', rentaiRate: 'higher', avgRawScore: 'higher'
    };

    Object.keys(statFields).forEach(field => {
        const values = rankedUsers.map(u => u[field]);
        minMax[field] = { min: Math.min(...values), max: Math.max(...values) };
    });

    rankedUsers.sort((a, b) => b.totalPoints - a.totalPoints);
    
    const getClass = (field, value) => {
        if (rankedUsers.length <= 1) return '';
        if (minMax[field].min === minMax[field].max) return '';
        if (statFields[field] === 'higher') {
            if (value === minMax[field].max) return 'text-rank-1';
            if (value === minMax[field].min) return 'text-rank-4';
        } else if (statFields[field] === 'lower') {
            if (value === minMax[field].min) return 'text-rank-1';
            if (value === minMax[field].max) return 'text-rank-4';
        }
        return '';
    };

    // Populate Desktop Table
    if (leaderboardBody) {
        leaderboardBody.innerHTML = rankedUsers.map((user, index) => {
            const photoHtml = getPlayerPhotoHtml(user.id, 'w-8 h-8');
            return `
                <tr class="hover:bg-gray-800 font-m-gothic text-xs md:text-sm">
                    <td class="px-2 py-4 whitespace-nowrap text-right sticky-col-1">${index + 1}</td>
                    <td class="px-2 py-4 whitespace-nowrap text-left font-medium text-blue-400 cursor-pointer hover:underline sticky-col-2" onclick="showPlayerStats('${user.id}')">
                        <div class="flex items-center gap-3">
                            ${photoHtml}
                            <span>${user.name}</span>
                        </div>
                    </td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right font-bold ${user.totalPoints >= 0 ? 'text-green-400' : 'text-red-400'}">${(user.totalPoints).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right">${user.totalHanchans.toLocaleString()}</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('avgRank', user.avgRank)}">${user.avgRank.toFixed(2)}</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('topRate', user.topRate)}">${user.topRate.toFixed(2)}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ">${user.ranks[1] > 0 ? ((user.ranks[1]/user.totalHanchans)*100).toFixed(2) : '0.00'}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ">${user.ranks[2] > 0 ? ((user.ranks[2]/user.totalHanchans)*100).toFixed(2) : '0.00'}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('lastRate', user.lastRate)}">${user.lastRate.toFixed(2)}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('rentaiRate', user.rentaiRate)}">${user.rentaiRate.toFixed(2)}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('bustedRate', user.bustedRate)}">${user.bustedRate.toFixed(2)}%</td>
                    <td class="px-2 md:px-4 py-4 whitespace-nowrap text-right ${getClass('avgRawScore', user.avgRawScore)}">${user.avgRawScore.toLocaleString()}</td>
                </tr>
            `;
        }).join('');
    }

    // Populate Mobile Cards
    if (cardsContainer) {
        cardsContainer.innerHTML = rankedUsers.map((user, index) => {
            const photoHtml = getPlayerPhotoHtml(user.id, 'w-12 h-12');
            return `
            <div class="cyber-card p-3" onclick="showPlayerStats('${user.id}')">
                <div class="flex justify-between items-center mb-3">
                    <div class="flex items-center gap-3">
                        <span class="text-xl font-bold text-gray-400 w-8 text-center">${index + 1}</span>
                        ${photoHtml}
                        <span class="font-bold text-lg text-blue-400">${user.name}</span>
                    </div>
                    <div class="text-right">
                        <p class="text-xs text-gray-400">合計Pt</p>
                        <p class="text-xl font-bold ${user.totalPoints >= 0 ? 'text-green-400' : 'text-red-400'}">${(user.totalPoints).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</p>
                    </div>
                </div>
                <div class="grid grid-cols-3 gap-3 text-center text-sm border-t border-gray-700 pt-3">
                    <div><p class="text-xs text-gray-400">平均着順</p><p class="font-bold ${getClass('avgRank', user.avgRank)}">${user.avgRank.toFixed(2)}</p></div>
                    <div><p class="text-xs text-gray-400">トップ率</p><p class="font-bold ${getClass('topRate', user.topRate)}">${user.topRate.toFixed(1)}%</p></div>
                    <div><p class="text-xs text-gray-400">ラス率</p><p class="font-bold ${getClass('lastRate', user.lastRate)}">${user.lastRate.toFixed(1)}%</p></div>
                    <div><p class="text-xs text-gray-400">半荘数</p><p class="font-bold">${user.totalHanchans}</p></div>
                    <div><p class="text-xs text-gray-400">連対率</p><p class="font-bold ${getClass('rentaiRate', user.rentaiRate)}">${user.rentaiRate.toFixed(1)}%</p></div>
                    <div><p class="text-xs text-gray-400">トビ率</p><p class="font-bold ${getClass('bustedRate', user.bustedRate)}">${user.bustedRate.toFixed(1)}%</p></div>
                </div>
            </div>
            `;
        }).join('');
    }
}

function renderHistoryTab() {
    const container = document.getElementById('history-tab');
    if (!container) return;
    container.innerHTML = `
        <h2 class="cyber-header text-2xl font-bold mb-4 border-b border-gray-700 pb-2 text-blue-400">対局履歴</h2>
        <div class="flex flex-col sm:flex-row gap-4 mb-4 p-4 bg-gray-900 rounded-lg">
            <div class="flex-1">
                <label for="history-year-filter" class="block text-sm font-medium text-gray-400">年</label>
                <select id="history-year-filter" onchange="updateHistoryList()" class="mt-1 block w-full rounded-md"></select>
            </div>
            <div class="flex-1">
                <label for="history-month-filter" class="block text-sm font-medium text-gray-400">月</label>
                <select id="history-month-filter" onchange="updateHistoryList()" class="mt-1 block w-full rounded-md"></select>
            </div>
            <div class="flex-1">
                <label for="history-player-filter" class="block text-sm font-medium text-gray-400">雀士</label>
                <select id="history-player-filter" onchange="updateHistoryList()" class="mt-1 block w-full rounded-md"></select>
            </div>
        </div>
        <div id="history-list-container" class="space-y-4"></div>
    `;
}

function updateHistoryTabFilters() {
    const yearSelect = document.getElementById('history-year-filter');
    const monthSelect = document.getElementById('history-month-filter');
    const playerSelect = document.getElementById('history-player-filter');

    if (!yearSelect || !monthSelect || !playerSelect) return;

    // Preserve selections
    const currentYear = yearSelect.value;
    const currentMonth = monthSelect.value;
    const currentPlayer = playerSelect.value;

    // Year filter
    const yearOptions = getGameYears().map(year => `<option value="${year}">${year}年</option>`).join('');
    yearSelect.innerHTML = `<option value="all">すべて</option>${yearOptions}`;
    if (Array.from(yearSelect.options).some(opt => opt.value === currentYear)) {
        yearSelect.value = currentYear;
    }

    // Month filter (static, but let's populate it here for consistency)
    if (monthSelect.options.length === 0) {
        const monthOptions = Array.from({length: 12}, (_, i) => i + 1).map(m => `<option value="${m}">${m}月</option>`).join('');
        monthSelect.innerHTML = `<option value="all">すべて</option>${monthOptions}`;
    }
    if (Array.from(monthSelect.options).some(opt => opt.value === currentMonth)) {
        monthSelect.value = currentMonth;
    }

    // Player filter
    const playerOptions = users.map(u => `<option value="${u.id}">${u.name}</option>`).join('');
    playerSelect.innerHTML = `<option value="all">すべて</option>${playerOptions}`;
    if (Array.from(playerSelect.options).some(opt => opt.value === currentPlayer)) {
        playerSelect.value = currentPlayer;
    }
}

window.updateHistoryList = () => {
    const container = document.getElementById('history-list-container');
    if (!container) return;

    const yearFilter = document.getElementById('history-year-filter').value;
    const monthFilter = document.getElementById('history-month-filter').value;
    const playerFilter = document.getElementById('history-player-filter').value;

    let filteredGames = [...games];

    // Filter by year
    if (yearFilter !== 'all') {
        filteredGames = filteredGames.filter(game => {
            const gameYear = (game.gameDate || '').substring(0, 4);
            return gameYear === yearFilter;
        });
    }

    // Filter by month
    if (monthFilter !== 'all') {
        filteredGames = filteredGames.filter(game => {
            if (!game.gameDate) return false;
            // Expected format: "YYYY/M/D..."
            const parts = game.gameDate.split('/');
            if (parts.length > 1) {
                const gameMonth = parts[1];
                return gameMonth === monthFilter;
            }
            return false;
        });
    }

    // Filter by player
    if (playerFilter !== 'all') {
        filteredGames = filteredGames.filter(game => game.playerIds.includes(playerFilter));
    }
    
    if (filteredGames.length === 0) {
        container.innerHTML = `<p class="text-gray-500 text-center py-8">該当する対局履歴がありません。</p>`;
        return;
    }

    container.innerHTML = filteredGames.map(game => {
        const date = game.gameDate || new Date(game.createdAt.seconds * 1000).toLocaleString('ja-JP');
        const winnerEntry = Object.entries(game.totalPoints).sort((a, b) => b[1] - a[1])[0];
        const winnerId = winnerEntry[0];
        const winnerUser = users.find(u => u.id === winnerId);
        const photoHtml = getPlayerPhotoHtml(winnerId, 'w-8 h-8');

        return `
            <div class="bg-gray-900 p-4 rounded-lg border border-gray-700 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
                <div class="cursor-pointer flex-grow" onclick="showGameDetails('${game.id}')">
                    <p class="font-bold text-lg">${date}</p>
                    <p class="text-sm text-gray-400">${game.playerNames.join(', ')}</p>
                </div>
                <div class="flex justify-between w-full sm:w-auto items-center">
                    <div class="text-left sm:text-right mr-4 cursor-pointer flex items-center gap-2" onclick="showGameDetails('${game.id}')">
                        ${photoHtml}
                        <div>
                            <p class="text-xs">WINNER</p>
                            <p class="font-bold text-green-400">${winnerUser ? winnerUser.name : 'N/A'} (+${winnerEntry[1].toFixed(1)})</p>
                        </div>
                    </div>
                    <div class="flex gap-2">
                        <button onclick="editGame('${game.id}')" class="text-blue-400 hover:text-blue-300 text-lg p-2 self-center"><i class="fas fa-edit"></i></button>
                        <button onclick="confirmDeleteGame('${game.id}', '${date}')" class="text-red-500 hover:text-red-400 text-lg p-2 self-center"><i class="fas fa-trash-alt"></i></button>
                    </div>
                </div>
            </div>
        `;
    }).join('');
};

function renderDetailedHistoryTables() {
    const rawListContainer = document.getElementById('history-raw-list');
    const ptListContainer = document.getElementById('history-pt-list');
    if (!rawListContainer || !ptListContainer) return;

    const allHanchans = [];
    games.forEach(game => {
        game.scores.forEach((hanchan, index) => {
            allHanchans.push({
                date: game.gameDate || new Date(game.createdAt.seconds * 1000).toLocaleString('ja-JP'),
                gameId: game.id,
                hanchanNum: index + 1,
                playerIds: game.playerIds,
                rawScores: hanchan.rawScores,
                points: hanchan.points
            });
        });
    });

    const createTable = (dataType, container, yearFilter, monthFilter, playerFilter) => {
         let filteredHanchans = [...allHanchans];

        if (yearFilter !== 'all') {
            filteredHanchans = filteredHanchans.filter(h => (h.date || '').substring(0, 4) === yearFilter);
        }
        if (monthFilter !== 'all') {
            filteredHanchans = filteredHanchans.filter(h => {
                if (!h.date) return false;
                const parts = h.date.split('/');
                return parts.length > 1 && parts[1] === monthFilter;
            });
        }
        if (playerFilter !== 'all') {
            filteredHanchans = filteredHanchans.filter(h => h.playerIds.includes(playerFilter));
        }

        let tableHtml = `<table class="min-w-full divide-y divide-gray-700 font-m-gothic text-xs md:text-sm">
            <thead class="bg-gray-900 text-xs md:text-sm font-medium text-gray-400 uppercase tracking-wider">
                <tr>
                    <th class="px-2 py-3 text-left whitespace-nowrap">日時</th>
                    ${users.map(u => `<th class="px-2 py-3 text-right whitespace-nowrap">${u.name}</th>`).join('')}
                </tr>
            </thead>
            <tbody class="divide-y divide-gray-700">`;
        
        if (filteredHanchans.length === 0) {
            tableHtml += `<tr><td colspan="${users.length + 1}" class="text-center py-4 text-gray-500">NO DATA</td></tr>`;
        } else {
            let lastDate = null;
            filteredHanchans.forEach(hanchan => {
                const currentDate = hanchan.date.split('(')[0];
                let borderClass = '';
                if (lastDate && currentDate !== lastDate) {
                    borderClass = 'border-t-2 border-gray-500';
                }
                lastDate = currentDate;

                const scores = hanchan[dataType];
                const scoreGroups = {};
                Object.entries(hanchan.rawScores).forEach(([pId, score]) => {
                    if (!scoreGroups[score]) scoreGroups[score] = [];
                    scoreGroups[score].push(pId);
                });
                const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
                
                const ranks = {};
                let rankCursor = 1;
                sortedScores.forEach(score => {
                    const playersInGroup = scoreGroups[score];
                    playersInGroup.forEach(pId => { ranks[pId] = rankCursor; });
                    rankCursor += playersInGroup.length;
                });
                
                tableHtml += `<tr class="${borderClass}"><td class="px-2 py-2 whitespace-nowrap">${hanchan.date} (#${hanchan.hanchanNum})</td>`;
                users.forEach(user => {
                    if (scores[user.id] !== undefined) {
                        const rank = ranks[user.id];
                        let rankClass = '';
                        if (rank === 1) rankClass = 'text-rank-1';
                        if (rank === 4) rankClass = 'text-rank-4';
                        tableHtml += `<td class="px-2 py-2 text-right ${rankClass}">${dataType === 'points' ? scores[user.id].toFixed(1) : scores[user.id].toLocaleString()}</td>`;
                    } else {
                        tableHtml += `<td class="px-2 py-2 text-right text-gray-600">-</td>`;
                    }
                });
                tableHtml += `</tr>`;
            });
        }

        tableHtml += `</tbody></table>`;
        container.innerHTML = tableHtml;
    };
    
    const rawYear = document.getElementById('history-raw-year-filter').value;
    const rawMonth = document.getElementById('history-raw-month-filter').value;
    const rawPlayer = document.getElementById('history-raw-player-filter').value;
    createTable('rawScores', rawListContainer, rawYear, rawMonth, rawPlayer);

    const ptYear = document.getElementById('history-pt-year-filter').value;
    const ptMonth = document.getElementById('history-pt-month-filter').value;
    const ptPlayer = document.getElementById('history-pt-player-filter').value;
    createTable('points', ptListContainer, ptYear, ptMonth, ptPlayer);
}

// --- Stats Page ---
window.showPlayerStats = (playerId) => {
    changeTab('personal-stats');
    const select = document.getElementById('personal-stats-player-select');
    select.value = playerId;
    displayPlayerStats(playerId);
};

window.displayPlayerStats = (playerId) => {
    const container = document.getElementById('personal-stats-content');
    if (!playerId) {
        container.innerHTML = `<p class="text-gray-500">雀士を選択して成績を表示します。</p>`;
        return;
    }
    const player = users.find(u => u.id === playerId);
    if (!player) return;
    
    const playerStats = cachedStats[playerId];
    if (!playerStats) return;

    const rankedUsers = Object.values(cachedStats).filter(u => u.totalHanchans > 0);

    const getRank = (metricKey, lowerIsBetter) => {
        if (rankedUsers.length === 0) return { rank: '-', total: 0 };
        const sortedUsers = [...rankedUsers].sort((a, b) => {
            return lowerIsBetter ? a[metricKey] - b[metricKey] : b[metricKey] - a[metricKey];
        });
        const rank = sortedUsers.findIndex(u => u.id === playerId) + 1;
        return { rank: rank > 0 ? rank : '-', total: rankedUsers.length };
    };

    const totalPointsRank = getRank('totalPoints', false);
    const avgRankRank = getRank('avgRank', true);
    const topRateRank = getRank('topRate', false);
    const rentaiRateRank = getRank('rentaiRate', false);
    const lastRateRank = getRank('lastRate', true);
    const avgRawScoreRank = getRank('avgRawScore', false);

    const photoHtml = getPlayerPhotoHtml(playerId, 'w-20 h-20');
    
    const comparisonOptions = users
        .filter(u => u.id !== playerId)
        .map(u => `
            <div class="mr-4">
                <input type="checkbox" id="compare-${u.id}" value="${u.id}" class="comparison-checkbox" onchange="updateComparisonCharts('${playerId}')">
                <label for="compare-${u.id}" class="ml-1">${u.name}</label>
            </div>
        `).join('');

    container.innerHTML = `
        <div class="flex items-center gap-4 mb-6">
            ${photoHtml}
            <h3 class="cyber-header text-3xl font-bold text-blue-400">${player.name}</h3>
        </div>

        <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 text-center mb-6">
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">合計Pt</p><p class="text-2xl font-bold ${playerStats.totalPoints >= 0 ? 'text-green-400' : 'text-red-400'}">${playerStats.totalPoints.toFixed(1)}</p><p class="text-xs text-gray-500 mt-1">全体 ${totalPointsRank.total}人中 ${totalPointsRank.rank}位</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">平均着順</p><p class="text-2xl font-bold">${playerStats.avgRank.toFixed(2)}</p><p class="text-xs text-gray-500 mt-1">全体 ${avgRankRank.total}人中 ${avgRankRank.rank}位</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">トップ率</p><p class="text-2xl font-bold text-green-400">${playerStats.topRate.toFixed(1)}%</p><p class="text-xs text-gray-500 mt-1">全体 ${topRateRank.total}人中 ${topRateRank.rank}位</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">連対率</p><p class="text-2xl font-bold">${playerStats.rentaiRate.toFixed(1)}%</p><p class="text-xs text-gray-500 mt-1">全体 ${rentaiRateRank.total}人中 ${rentaiRateRank.rank}位</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">ラス率</p><p class="text-2xl font-bold text-red-400">${playerStats.lastRate.toFixed(1)}%</p><p class="text-xs text-gray-500 mt-1">全体 ${lastRateRank.total}人中 ${lastRateRank.rank}位</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">平均素点</p><p class="text-2xl font-bold">${playerStats.avgRawScore.toLocaleString()}</p><p class="text-xs text-gray-500 mt-1">全体 ${avgRawScoreRank.total}人中 ${avgRawScoreRank.rank}位</p></div>
        </div>

        <div class="cyber-card p-4 mb-6">
            <h4 class="font-bold mb-2">比較対象</h4>
            <div id="comparison-checkboxes" class="flex flex-wrap">${comparisonOptions}</div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <div class="cyber-card p-4 sm:p-6">
                <h3 class="cyber-header text-xl font-bold mb-4 text-center">順位分布</h3>
                <div class="w-full h-64 mx-auto"><canvas id="rank-chart-personal"></canvas></div>
            </div>
            <div class="cyber-card p-4 sm:p-6">
                <h3 class="cyber-header text-xl font-bold mb-4 text-center">ポイント推移</h3>
                <div class="w-full h-64 mx-auto"><canvas id="point-history-chart-personal"></canvas></div>
            </div>
        </div>
        <div id="player-history-container" class="cyber-card p-4 sm:p-6"></div>
    `;
    
    renderStatsCharts(playerId, []);
    renderPlayerHistoryTable(playerId);
}

window.updateComparisonCharts = (mainPlayerId) => {
    const checkedBoxes = document.querySelectorAll('#comparison-checkboxes input:checked');
    const comparisonIds = Array.from(checkedBoxes).map(cb => cb.value);
    renderStatsCharts(mainPlayerId, comparisonIds);
};

function renderPlayerHistoryTable(playerId) {
    const container = document.getElementById('player-history-container');
    if (!container) return;
    
    const playerHanchans = [];
    games.forEach(game => {
        if (game.playerIds.includes(playerId)) {
            game.scores.forEach((hanchan, index) => {
                const scoreGroups = {};
                Object.entries(hanchan.rawScores).forEach(([pId, score]) => {
                    if (!scoreGroups[score]) scoreGroups[score] = [];
                    scoreGroups[score].push(pId);
                });
                const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
                
                const ranks = {};
                let rankCursor = 1;
                sortedScores.forEach(score => {
                    const playersInGroup = scoreGroups[score];
                    playersInGroup.forEach(pId => {
                        ranks[pId] = rankCursor;
                    });
                    rankCursor += playersInGroup.length;
                });

                playerHanchans.push({
                    date: game.gameDate || new Date(game.createdAt.seconds * 1000).toLocaleString('ja-JP'),
                    rawScore: hanchan.rawScores[playerId],
                    point: hanchan.points[playerId],
                    rank: ranks[playerId]
                });
            });
        }
    });

    if (playerHanchans.length === 0) {
        container.innerHTML = `<h3 class="cyber-header text-xl font-bold mb-4 text-center">対局履歴</h3><p class="text-gray-500 text-center">まだ対局記録がありません。</p>`;
        return;
    }

    let tableHtml = `<h3 class="cyber-header text-xl font-bold mb-4 text-center">対局履歴</h3>
        <div class="overflow-x-auto max-h-96">
            <table class="min-w-full divide-y divide-gray-700 text-sm">
                <thead class="bg-gray-900 sticky top-0"><tr>
                    <th class="px-4 py-2 text-left">日時</th>
                    <th class="px-4 py-2 text-right">着順</th>
                    <th class="px-4 py-2 text-right">素点</th>
                    <th class="px-4 py-2 text-right">ポイント</th>
                </tr></thead>
                <tbody class="divide-y divide-gray-700">`;
    
    playerHanchans.reverse().forEach(h => {
        tableHtml += `<tr>
            <td class="px-4 py-2 whitespace-nowrap">${h.date}</td>
            <td class="px-4 py-2 text-right">${h.rank}</td>
            <td class="px-4 py-2 text-right">${h.rawScore.toLocaleString()}</td>
            <td class="px-4 py-2 text-right ${h.point >= 0 ? 'text-green-400' : 'text-red-400'}">${h.point.toFixed(1)}</td>
        </tr>`;
    });

    tableHtml += `</tbody></table></div>`;
    container.innerHTML = tableHtml;
}

function renderStatsCharts(mainPlayerId, comparisonIds) {
    const colors = ['#58a6ff', '#52c569', '#f5655f', '#f2cc8f', '#e0aaff', '#9bf6ff'];
    
    // --- Point History Chart (Personal Tab) ---
    const personalChartCanvas = document.getElementById('point-history-chart-personal');
    if (personalChartCanvas) {
        const playerIdsForChart = [mainPlayerId, ...comparisonIds];
        const relevantGames = games.filter(g => g.playerIds.some(pId => playerIdsForChart.includes(pId)));
        const dateStrings = relevantGames.map(g => g.gameDate.split('(')[0]);
        const today = new Date();
        const todayString = `${today.getFullYear()}/${today.getMonth() + 1}/${today.getDate()}`;
        dateStrings.push(todayString);
        const fullTimeline = [...new Set(dateStrings)].sort((a, b) => new Date(a) - new Date(b));

        const pointHistoryDatasets = [];
        const mainPlayerData = getPlayerPointHistory(mainPlayerId, fullTimeline);
        pointHistoryDatasets.push({
            label: cachedStats[mainPlayerId].name,
            data: mainPlayerData,
            borderColor: colors[0],
            backgroundColor: colors[0] + '33',
            fill: true, tension: 0.1
        });
        comparisonIds.forEach((id, index) => {
            const playerData = getPlayerPointHistory(id, fullTimeline);
            pointHistoryDatasets.push({
                label: cachedStats[id].name,
                data: playerData,
                borderColor: colors[(index + 1) % colors.length],
                backgroundColor: colors[(index + 1) % colors.length] + '33',
                fill: true, tension: 0.1
            });
        });

        if (personalPointHistoryChart) personalPointHistoryChart.destroy();
        personalPointHistoryChart = new Chart(personalChartCanvas.getContext('2d'), {
            type: 'line',
            data: { labels: fullTimeline, datasets: pointHistoryDatasets },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#c9d1d9' }}}, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } } } }
        });
    }

    // --- Rank Distribution Chart (Personal Tab) ---
    const rankChartCanvas = document.getElementById('rank-chart-personal');
    if(rankChartCanvas) {
        const rankDatasets = [];
        rankDatasets.push({
            label: cachedStats[mainPlayerId].name,
            data: cachedStats[mainPlayerId].ranks,
            backgroundColor: colors[0],
        });
        comparisonIds.forEach((id, index) => {
            rankDatasets.push({
                label: cachedStats[id].name,
                data: cachedStats[id].ranks,
                backgroundColor: colors[(index + 1) % colors.length],
            });
        });

        if (personalRankChart) personalRankChart.destroy();
        personalRankChart = new Chart(rankChartCanvas.getContext('2d'), {
            type: 'bar',
            data: { labels: ['1位', '2位', '3位', '4位'], datasets: rankDatasets },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top', labels: { color: '#c9d1d9' }}}, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } } } }
        });
    }
}

window.updateDataAnalysisCharts = function() {
    if (!document.getElementById('data-analysis-tab') || document.getElementById('data-analysis-tab').classList.contains('hidden')) {
        return;
    }

    const rankedUsers = Object.values(cachedStats).filter(u => u.totalHanchans > 0);
    const colors = ['#58a6ff', '#52c569', '#f5655f', '#E2FF08', '#e0aaff', '#9bf6ff', '#ffb700', '#00ffc8'];
    const allPlayers = [...rankedUsers].sort((a,b) => b.totalPoints - a.totalPoints);

    // --- 1. Stat Cards ---
    const statCardsContainer = document.getElementById('stat-cards-container');
    if (rankedUsers.length > 0) {
        const totalHanchans = rankedUsers.reduce((sum, u) => sum + u.totalHanchans, 0);
        const gameDays = new Set(games.map(g => g.gameDate.split('(')[0])).size;
        const leader = allPlayers[0];

        const topHanchans = [...rankedUsers].sort((a,b) => b.totalHanchans - a.totalHanchans)[0];
        const hanchanParticipationRate = totalHanchans > 0 ? (topHanchans.totalHanchans / totalHanchans * 100).toFixed(1) : 0;
        
        const participationDays = {};
        users.forEach(u => participationDays[u.id] = new Set());
        games.forEach(g => {
            const date = g.gameDate.split('(')[0];
            g.playerIds.forEach(pId => participationDays[pId].add(date));
        });
        const topDays = [...rankedUsers].sort((a,b) => participationDays[b.id].size - participationDays[a.id].size)[0];
        const daysParticipationRate = gameDays > 0 ? (participationDays[topDays.id].size / gameDays * 100).toFixed(1) : 0;

        let highestHanchan = { score: -Infinity, name: '', id: '' };
        games.forEach(g => g.scores.forEach(s => Object.entries(s.rawScores).forEach(([pId, score]) => {
            if (score > highestHanchan.score) {
                highestHanchan = { score, name: users.find(u=>u.id===pId)?.name, id: pId };
            }
        })));

        const dailyPoints = {};
        games.forEach(g => {
            const date = g.gameDate.split('(')[0];
            if(!dailyPoints[date]) dailyPoints[date] = {};
            Object.entries(g.totalPoints).forEach(([pId, pts]) => {
                if(!dailyPoints[date][pId]) dailyPoints[date][pId] = 0;
                dailyPoints[date][pId] += pts;
            });
        });
        let highestDaily = { pt: -Infinity, name: '', id: '' };
        Object.values(dailyPoints).forEach(day => Object.entries(day).forEach(([pId, pt]) => {
             if (pt > highestDaily.pt) {
                 highestDaily = { pt, name: users.find(u=>u.id===pId)?.name, id: pId };
            }
        }));

        const getColorForPlayer = (playerId) => {
            const rankIndex = allPlayers.findIndex(p => p.id === playerId);
            if (rankIndex === -1) return 'var(--accent-green)'; // Fallback for players not in ranked list
            return colors[rankIndex % colors.length];
        };

        statCardsContainer.innerHTML = `
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">総半荘数</p><p class="text-2xl font-bold" style="color: var(--accent-green);">${totalHanchans}</p><p class="text-xs text-gray-500">開催日数: ${gameDays}日</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">現時点トップ</p><p class="text-xl font-bold" style="color: ${getColorForPlayer(leader.id)};">${leader.name}</p><p class="text-xs">${leader.totalPoints.toFixed(1)} pt</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">参加半荘数トップ</p><p class="text-xl font-bold" style="color: ${getColorForPlayer(topHanchans.id)};">${topHanchans.name}</p><p class="text-xs">参加 ${topHanchans.totalHanchans}半荘：参加率 ${hanchanParticipationRate}％</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">参加日数トップ</p><p class="text-xl font-bold" style="color: ${getColorForPlayer(topDays.id)};">${topDays.name}</p><p class="text-xs">参加 ${participationDays[topDays.id].size}日：参加率 ${daysParticipationRate}％</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">1半荘最高素点</p><p class="text-xl font-bold" style="color: ${getColorForPlayer(highestHanchan.id)};">${highestHanchan.name}</p><p class="text-xs">${highestHanchan.score.toLocaleString()}点</p></div>
            <div class="cyber-card p-3"><p class="text-sm text-gray-400">1日最高獲得Pt</p><p class="text-xl font-bold" style="color: ${getColorForPlayer(highestDaily.id)};">${highestDaily.name}</p><p class="text-xs">+${highestDaily.pt.toFixed(1)} pt</p></div>
        `;
    } else {
        statCardsContainer.innerHTML = `<div class="col-span-full text-center text-gray-500 py-4">データがありません</div>`;
        return; // No data, no charts
    }
    
    // --- Top 3 Players ---
    const top3Container = document.getElementById('top-3-container');
    if (top3Container && allPlayers.length > 0) {
        const top3 = allPlayers.slice(0, 3);
        top3Container.innerHTML = `
            <h3 class="cyber-header text-xl font-bold mb-4 text-center text-yellow-300">現時点トップ３</h3>
            <div class="flex justify-around items-end gap-4">
                ${top3.map((p, i) => {
                    const rankClass = i === 0 ? 'text-rank-gold' : (i === 1 ? 'text-rank-silver' : 'text-rank-bronze');
                    const sizeClass = i === 0 ? 'w-24 h-24' : (i === 1 ? 'w-20 h-20' : 'w-16 h-16');
                    const nameSize = i === 0 ? 'text-lg' : (i === 1 ? 'text-base' : 'text-sm');
                    return `
                    <div class="text-center flex flex-col items-center">
                        <span class="font-bold text-2xl ${rankClass}">${i+1}</span>
                        ${getPlayerPhotoHtml(p.id, sizeClass)}
                        <span class="font-bold ${nameSize} mt-2 text-blue-400">${p.name}</span>
                        <span class="text-sm ${p.totalPoints >= 0 ? 'text-green-400' : 'text-red-400'}">${p.totalPoints.toFixed(1)} pt</span>
                    </div>
                    `
                }).join('')}
            </div>
        `;
    } else if (top3Container) {
        top3Container.innerHTML = '';
    }

    // --- 2. Radar Chart ---
    const radarPlayerSelect = document.getElementById('radar-player-select');
    const rankedUsersForSelection = [...rankedUsers].sort((a, b) => a.name.localeCompare(b.name, 'ja'));

    let selectedRadarPlayerIds;
    const existingCheckboxes = document.querySelectorAll('.radar-checkbox');
    if (existingCheckboxes.length > 0 && Array.from(existingCheckboxes).some(cb => cb.checked)) {
        selectedRadarPlayerIds = Array.from(existingCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
    } else {
        selectedRadarPlayerIds = allPlayers.map(p => p.id);
    }

    radarPlayerSelect.innerHTML = rankedUsersForSelection.map(u => {
        const isChecked = selectedRadarPlayerIds.includes(u.id);
        const playerIndexInRankedList = allPlayers.findIndex(p => p.id === u.id);
        const color = colors[playerIndexInRankedList % colors.length];
        
        return `
            <label for="radar-check-${u.id}" class="flex items-center cursor-pointer p-1 rounded hover:bg-gray-800 transition-colors">
                <input type="checkbox" id="radar-check-${u.id}" value="${u.id}" class="radar-checkbox" onchange="updateDataAnalysisCharts()" ${isChecked ? 'checked' : ''} style="display:none;">
                <span class="inline-block w-3 h-3 rounded-full mr-2 border border-gray-600" style="background-color: ${isChecked ? color : 'transparent'};"></span>
                <span>${u.name}</span>
            </label>
        `;
    }).join('');

    const radarLabels = ['平均素点', 'トップ率', '連対率', 'ラス回避率', '平均順位'];
    const radarStatKeys = { '平均素点': 'avgRawScore', 'トップ率': 'topRate', '連対率': 'rentaiRate', 'ラス回避率': 'lastRate', '平均順位': 'avgRank' };
    
    const radarMinMax = {};
    Object.values(radarStatKeys).forEach(key => {
        const values = rankedUsers.map(u => u[key]);
        radarMinMax[key] = { min: Math.min(...values), max: Math.max(...values) };
    });
    // For inverted stats
    radarMinMax.lastRate.min = 0; radarMinMax.lastRate.max = 100; // Rate is 0-100
    radarMinMax.avgRank.min = 1; radarMinMax.avgRank.max = 4; // Rank is 1-4

    const normalize = (value, key) => {
        const { min, max } = radarMinMax[key];
        if (max === min) return 50;
        // Invert score for "lower is better" stats
        if (key === 'lastRate') return 100 * ( (max - value) / (max - min) );
        if (key === 'avgRank') return 100 * ( (max - value) / (max - min) );
        return 100 * ( (value - min) / (max - min) );
    };
    
    const radarDatasets = selectedRadarPlayerIds.map(pId => {
        const userStats = rankedUsers.find(u => u.id === pId);
        const playerIndexInRankedList = allPlayers.findIndex(p => p.id === pId);
        const color = colors[playerIndexInRankedList % colors.length];
        
        return {
            label: userStats.name,
            data: radarLabels.map(label => normalize(userStats[radarStatKeys[label]], radarStatKeys[label])),
            borderColor: color,
            backgroundColor: color + '33',
            pointBackgroundColor: color,
        };
    });

    if (playerRadarChart) playerRadarChart.destroy();
    playerRadarChart = new Chart(document.getElementById('player-radar-chart').getContext('2d'), {
        type: 'radar',
        data: { labels: radarLabels, datasets: radarDatasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { 
                legend: { 
                    display: false 
                } 
            },
            scales: { r: { angleLines: { color: '#30363d' }, grid: { color: '#30363d' }, pointLabels: { color: '#c9d1d9' }, suggestedMin: 0, suggestedMax: 100, ticks: { display: false } } }
        }
    });

    // --- 3. Point History Chart ---
    const dateStrings = games.map(g => g.gameDate.split('(')[0]);
    const today = new Date();
    const todayString = `${today.getFullYear()}/${today.getMonth() + 1}/${today.getDate()}`;
    dateStrings.push(todayString);
    const fullTimeline = [...new Set(dateStrings)].sort((a, b) => new Date(a) - new Date(b));

    const pointHistoryDatasets = allPlayers.map((player, index) => {
        const history = getPlayerPointHistory(player.id, fullTimeline);
        return {
            label: player.name,
            data: history,
            borderColor: colors[index % colors.length],
            backgroundColor: colors[index % colors.length] + '33',
            fill: false, 
            tension: 0.1
        }
    });

    const pointHistoryCanvas = document.getElementById('point-history-chart');
    if (pointHistoryCanvas) {
        if (pointHistoryChart) pointHistoryChart.destroy();
        pointHistoryChart = new Chart(pointHistoryCanvas.getContext('2d'), {
            type: 'line',
            data: { labels: fullTimeline, datasets: pointHistoryDatasets },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#c9d1d9' }}}, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } } } }
        });
    }

    // --- 4. Bar Chart ---
    const barChartMetricSelect = document.getElementById('bar-chart-metric-select');
    const metrics = { 'totalPoints': '合計Pt', 'avgRawScore': '平均素点', 'topRate': 'トップ率', 'lastRate': 'ラス率', 'rentaiRate': '連対率', 'bustedRate': 'トビ率', 'avgRank': '平均順位', 'yakumanCount': '役満回数' };
    if (barChartMetricSelect.options.length === 0) {
         Object.entries(metrics).forEach(([key, value]) => {
             barChartMetricSelect.add(new Option(value, key));
        });
    }
    const selectedMetric = barChartMetricSelect.value || 'totalPoints';
    const lowerIsBetter = ['lastRate', 'bustedRate', 'avgRank'].includes(selectedMetric);
    
    const sortedBarUsers = [...rankedUsers].sort((a, b) => lowerIsBetter ? a[selectedMetric] - b[selectedMetric] : b[selectedMetric] - a[selectedMetric]);

    if (playerBarChart) playerBarChart.destroy();
    playerBarChart = new Chart(document.getElementById('player-bar-chart').getContext('2d'), {
        type: 'bar',
        data: {
            labels: sortedBarUsers.map(u => {
                const rank = allPlayers.findIndex(p => p.id === u.id) + 1;
                return `(${rank}位) ${u.name}`;
            }),
            datasets: [{
                label: metrics[selectedMetric],
                data: sortedBarUsers.map(u => u[selectedMetric]),
                backgroundColor: sortedBarUsers.map((u, i) => {
                    const rankIndex = allPlayers.findIndex(p => p.id === u.id);
                    return colors[rankIndex % colors.length] + 'AA';
                }),
                borderColor: sortedBarUsers.map((u, i) => {
                     const rankIndex = allPlayers.findIndex(p => p.id === u.id);
                     return colors[rankIndex % colors.length];
                }),
                borderWidth: 1
            }]
        },
        options: {
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#c9d1d9' }, grid: { color: '#30363d' } } }
        }
    });
}

// --- User & Game Management ---

window.addUser = async () => {
    const nameInput = document.getElementById('new-user-name');
    const name = nameInput.value.trim();
    if (!name) { showModalMessage("雀士名を入力してください。"); return; }
    if (users.some(u => u.name === name)) { showModalMessage("同じ名前の雀士が既に存在します。"); return; }
    try {
        const usersCollectionRef = collection(db, `users`);
        await addDoc(usersCollectionRef, { name: name, createdAt: new Date(), photoURL: null });
        nameInput.value = '';
        showModalMessage(`「${name}」さんを登録しました。`);
    } catch (error) { console.error("Error adding user: ", error); showModalMessage("ユーザーの追加に失敗しました。"); }
};

window.handlePhotoUpload = async (userId, inputElement) => {
    const file = inputElement.files[0];
    if (!file) return;

    showLoadingModal("写真をアップロード中...");

    const storageRef = ref(storage, `user-photos/${userId}/${file.name}`);

    try {
        const snapshot = await uploadBytes(storageRef, file);
        const downloadURL = await getDownloadURL(snapshot.ref);

        const userDocRef = doc(db, 'users', userId);
        await updateDoc(userDocRef, { photoURL: downloadURL });
        
        closeModal();

    } catch (error) {
        console.error("Photo upload failed:", error);
        showModalMessage("写真のアップロードに失敗しました。");
    }
};

async function executeUserNameUpdate(userId, newName) {
    const userDocRef = doc(db, 'users', userId);
    const batch = writeBatch(db);

    batch.update(userDocRef, { name: newName });

    const gamesQuery = query(collection(db, 'games'), where('playerIds', 'array-contains', userId));
    const gamesSnapshot = await getDocs(gamesQuery);
    
    gamesSnapshot.forEach(gameDoc => {
        const gameData = gameDoc.data();
        const playerIndex = gameData.playerIds.indexOf(userId);
        if (playerIndex !== -1) {
            const newPlayerNames = [...gameData.playerNames];
            newPlayerNames[playerIndex] = newName;
            batch.update(gameDoc.ref, { playerNames: newPlayerNames });
        }
    });

    await batch.commit();
}

window.toggleEditUser = async (userId) => {
    const input = document.getElementById(`user-name-input-${userId}`);
    const editBtn = document.getElementById(`edit-user-btn-${userId}`);
    const originalName = input.dataset.originalName;

    if (input.readOnly) {
        input.readOnly = false;
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
        editBtn.innerHTML = '<i class="fas fa-save"></i>';
        editBtn.classList.add('cyber-btn-green');
    } else {
        const newName = input.value.trim();

        if (!newName) {
            showModalMessage("雀士名は空にできません。");
            input.value = originalName;
        } else if (newName !== originalName && users.some(u => u.name === newName && u.id !== userId)) {
            showModalMessage("同じ名前の雀士が既に存在します。");
            input.value = originalName;
        } else if (newName !== originalName) {
            try {
                await executeUserNameUpdate(userId, newName);
                const playerInGame = selectedPlayers.find(p => p.id === userId);
                if(playerInGame) playerInGame.name = newName;
                showModalMessage("雀士名を更新しました。");
                input.dataset.originalName = newName;
            } catch (error) {
                console.error("Error updating user name: ", error);
                showModalMessage("名前の更新に失敗しました。");
                input.value = originalName;
            }
        }
        
        input.readOnly = true;
        editBtn.innerHTML = '<i class="fas fa-edit"></i>';
        editBtn.classList.remove('cyber-btn-green');
    }
};

window.confirmDeleteUser = (id) => {
    const user = users.find(u => u.id === id);
    if (!user) return;
    showModal(`
        <h3 class="cyber-header text-xl font-bold text-red-400 mb-4">削除確認</h3>
        <p>「${user.name}」を削除しますか？<br>関連する全ての対局データも永久に削除されます。この操作は元に戻せません。</p>
        <div class="flex justify-end gap-4 mt-6">
            <button onclick="closeModal()" class="cyber-btn px-4 py-2">キャンセル</button>
            <button onclick="executeDeleteUser('${id}')" class="cyber-btn-red px-4 py-2">削除実行</button>
        </div>
    `);
};

window.executeDeleteUser = async (id) => {
    closeModal();
    try {
        const batch = writeBatch(db);
        const gamesQuery = query(collection(db, `games`), where('playerIds', 'array-contains', id));
        const gamesSnapshot = await getDocs(gamesQuery);
        gamesSnapshot.forEach(doc => batch.delete(doc.ref));
        const userDocRef = doc(db, `users`, id);
        batch.delete(userDocRef);
        await batch.commit();
        showModalMessage(`ユーザーと関連データを削除しました。`);
        resetGame();
    } catch (error) { console.error("Error deleting user:", error); showModalMessage("削除に失敗しました。"); }
};

window.confirmDeleteGame = (gameId, date) => {
    showModal(`
        <h3 class="cyber-header text-xl font-bold text-red-400 mb-4">対局履歴 削除確認</h3>
        <p>${date} の対局データを削除しますか？<br>この操作は元に戻せません。</p>
        <div class="flex justify-end gap-4 mt-6">
            <button onclick="closeModal()" class="cyber-btn px-4 py-2">キャンセル</button>
            <button onclick="executeDeleteGame('${gameId}')" class="cyber-btn-red px-4 py-2">削除実行</button>
        </div>
    `);
};

window.executeDeleteGame = async (gameId) => {
    closeModal();
    try {
        await deleteDoc(doc(db, "games", gameId));
        showModalMessage("対局データを削除しました。");
    } catch (error) {
        console.error("Error deleting game: ", error);
        showModalMessage("対局データの削除に失敗しました。");
    }
};

window.editGame = (gameId) => {
    const game = games.find(g => g.id === gameId);
    if (!game) {
        showModalMessage("ゲームが見つかりません。");
        return;
    }

    // Set global state
    editingGameId = gameId;
    selectedPlayers = game.playerIds.map(pId => {
        const user = users.find(u => u.id === pId);
        return { id: user.id, name: user.name, photoURL: user.photoURL };
    });
    hanchanScores = game.scores.map(s => ({
        rawScores: s.rawScores,
        yakumanEvents: s.yakumanEvents || [],
        penalties: s.penalties || []
    }));

    // Switch to game tab
    changeTab('game');

    // --- Populate Step 1 (and lock it) ---
    renderPlayerSelection();
    document.getElementById('to-step2-btn').disabled = false;
    lockUnlockStep(1, true);

    // --- Populate Step 2 (and lock it) ---
    document.getElementById('step2-rule-settings').classList.remove('hidden');
    document.getElementById('base-point').value = game.settings.basePoint;
    document.getElementById('return-point').value = game.settings.returnPoint;
    document.getElementById('uma-1').value = game.settings.uma[0];
    document.getElementById('uma-2').value = game.settings.uma[1];
    document.getElementById('uma-3').value = game.settings.uma[2];
    document.getElementById('uma-4').value = game.settings.uma[3];
    updateOkaDisplay();
    lockUnlockStep(2, true);

    // --- Populate Step 3 ---
    document.getElementById('step3-score-input').classList.remove('hidden');
    document.getElementById('game-date').value = game.gameDate;
    renderScoreDisplay();
    
    // Update save button
    const saveBtn = document.getElementById('save-game-btn');
    saveBtn.innerHTML = `<i class="fas fa-sync-alt mr-2"></i>Pt変換して更新`;
    saveBtn.classList.remove('cyber-btn-yellow');
    saveBtn.classList.add('cyber-btn-green');

    document.getElementById('step3-score-input').scrollIntoView({ behavior: 'smooth', block: 'start' });
};

// --- Game Flow Functions ---

function calculateHanchanRanksAndPoints(scores) {
    const result = { points: {}, rawRanks: {}, pointRanks: {} };
    if (Object.values(scores).some(s => s === null || s === '')) {
        return result;
    }

    const basePoint = Number(document.getElementById('base-point').value);
    const returnPoint = Number(document.getElementById('return-point').value);
    const uma = [
        Number(document.getElementById('uma-1').value), Number(document.getElementById('uma-2').value),
        Number(document.getElementById('uma-3').value), Number(document.getElementById('uma-4').value)
    ];
    const oka = ((returnPoint - basePoint) * 4) / 1000;

    const scoreGroups = {};
    Object.entries(scores).forEach(([playerId, score]) => {
        const s = Number(score);
        if (!scoreGroups[s]) scoreGroups[s] = [];
        scoreGroups[s].push(playerId);
    });
    const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
    
    let rankCursor = 1;
    sortedScores.forEach(score => {
        const playersInGroup = scoreGroups[score];
        playersInGroup.forEach(pId => { result.rawRanks[pId] = rankCursor; });
        rankCursor += playersInGroup.length;
    });

    rankCursor = 0;
    sortedScores.forEach(score => {
        const playersInGroup = scoreGroups[score];
        const groupSize = playersInGroup.length;
        const umaSlice = uma.slice(rankCursor, rankCursor + groupSize);
        const totalUma = umaSlice.reduce((a, b) => a + b, 0);
        const sharedUma = totalUma / groupSize;
        let sharedOka = 0;
        if (rankCursor === 0) sharedOka = oka / groupSize;

        playersInGroup.forEach(playerId => {
            const pointWithoutUma = (Number(score) - returnPoint) / 1000;
            result.points[playerId] = pointWithoutUma + sharedUma + sharedOka;
        });
        rankCursor += groupSize;
    });

    const pointGroups = {};
    Object.entries(result.points).forEach(([playerId, p]) => {
        if (!pointGroups[p]) pointGroups[p] = [];
        pointGroups[p].push(playerId);
    });
    const sortedPoints = Object.keys(pointGroups).map(Number).sort((a, b) => b - a);
    rankCursor = 1;
    sortedPoints.forEach(p => {
        const playersInGroup = pointGroups[p];
        playersInGroup.forEach(pId => { result.pointRanks[pId] = rankCursor; });
        rankCursor += playersInGroup.length;
    });

    return result;
}

function renderScoreDisplay() {
    const container = document.getElementById('score-display-area');
    if (!container) return;
    container.innerHTML = hanchanScores.map((hanchan, index) => {
        const scores = hanchan.rawScores;
        const total = Object.values(scores).reduce((sum, score) => sum + (Number(score) || 0), 0);
        const basePoint = Number(document.getElementById('base-point').value);
        const isComplete = Object.values(scores).every(s => s !== null && s !== '');
        const totalColor = isComplete && total !== basePoint * 4 ? 'text-red-500' : (isComplete ? 'text-green-400' : '');

        const { points, rawRanks, pointRanks } = calculateHanchanRanksAndPoints(scores);

        const yakumanHtml = (hanchan.yakumanEvents && hanchan.yakumanEvents.length > 0)
            ? `<div class="mt-2 border-t border-gray-700 pt-2">
                ${hanchan.yakumanEvents.map(y => {
                    const user = users.find(u => u.id === y.playerId);
                    return `<span class="text-xs inline-block bg-yellow-900 text-yellow-300 rounded-full px-2 py-1 mr-1 mb-1">${user ? user.name : ''}: ${y.yakumans.join(', ')}</span>`
                }).join('')}
               </div>`
            : '';

        const penaltyHtml = (hanchan.penalties && hanchan.penalties.length > 0)
            ? `<div class="mt-2 border-t border-gray-700 pt-2">
                ${hanchan.penalties.map(p => {
                    const user = users.find(u => u.id === p.playerId);
                    const typeText = p.type === 'chombo' ? 'チョンボ' : 'アガリ放棄';
                    return `<span class="text-xs inline-block bg-red-900 text-red-300 rounded-full px-2 py-1 mr-1 mb-1">${user ? user.name : ''}: ${typeText} (${p.reason}) x${p.count}</span>`
                }).join('')}
               </div>`
            : '';

        return `
        <div class="cyber-card p-3" id="hanchan-display-${index}">
            <div class="flex justify-between items-center mb-2">
                <h4 class="font-bold text-lg">半荘 #${index + 1}</h4>
                <div class="flex gap-2">
                    <button onclick="openScoreInputModal(${index})" class="cyber-btn px-3 py-1 text-sm"><i class="fas fa-edit mr-1"></i>編集</button>
                    <button onclick="deleteHanchan(${index})" class="cyber-btn-red px-3 py-1 text-sm"><i class="fas fa-trash"></i></button>
                </div>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left">
                    <thead>
                        <tr class="border-b border-gray-700 text-xs text-gray-400">
                            <th class="p-1 font-normal w-1/3">雀士</th>
                            <th class="p-1 text-right font-normal">素点</th>
                            <th class="p-1 text-right font-normal">Pt</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${selectedPlayers.map(p => {
                            const rawScoreRank = rawRanks[p.id];
                            const rawScoreClass = rawScoreRank === 1 ? 'text-rank-1' : (rawScoreRank === 4 ? 'text-rank-4' : '');
                            const pointRank = pointRanks[p.id];
                            const pointClass = pointRank === 1 ? 'text-rank-1' : (pointRank === 4 ? 'text-rank-4' : '');
                            const pointValue = points[p.id];

                            return `
                            <tr>
                                <td class="p-1 font-medium">${p.name}</td>
                                <td class="p-1 text-right font-m-gothic ${rawScoreClass}">${scores[p.id] !== null ? Number(scores[p.id]).toLocaleString() : '-'}</td>
                                <td class="p-1 text-right font-m-gothic ${pointClass}">${pointValue !== undefined ? pointValue.toFixed(1) : '-'}</td>
                            </tr>
                        `}).join('')}
                    </tbody>
                    <tfoot>
                        <tr class="border-t border-gray-700">
                            <th class="p-1">合計</th>
                            <th class="p-1 text-right font-bold ${totalColor}">${total.toLocaleString()}</th>
                            <th class="p-1 text-right font-bold">0.0</th>
                        </tr>
                    </tfoot>
                </table>
                ${yakumanHtml}
                ${penaltyHtml}
            </div>
        </div>
        `;
    }).join('');
}

function setupScoreDisplay() {
    hanchanScores = [];
    addHanchan();
}

window.addHanchan = () => {
    const newHanchan = { rawScores: {}, yakumanEvents: [], penalties: [] };
    selectedPlayers.forEach(p => {
        newHanchan.rawScores[p.id] = null;
    });
    hanchanScores.push(newHanchan);
    renderScoreDisplay();
    setTimeout(() => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
    }, 100);
};

window.deleteHanchan = (index) => {
    if (hanchanScores.length > 1) {
        hanchanScores.splice(index, 1);
        renderScoreDisplay();
    } else {
        showModalMessage("最低1半荘は必要です。");
    }
};

window.openScoreInputModal = (index) => {
    const hanchan = hanchanScores[index];
    const scores = hanchan.rawScores;
    
    const modalHtml = `
        <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">半荘 #${index + 1} スコア入力</h3>
        <div class="grid md:grid-cols-2 gap-4">
            <div class="space-y-3">
                ${selectedPlayers.map(p => `
                    <div>
                        <label class="flex items-center gap-2 mb-1">
                            ${getPlayerPhotoHtml(p.id, 'w-8 h-8')}
                            <span class="text-sm font-medium text-gray-400">${p.name}</span>
                        </label>
                        <input type="text" id="modal-score-${p.id}" 
                               class="modal-score-input block w-full shadow-sm sm:text-lg p-2 text-right border-2 border-transparent bg-zinc-800" 
                               value="${scores[p.id] !== null ? scores[p.id] : ''}" 
                               onfocus="setActiveInput('modal-score-${p.id}')"
                               readonly>
                    </div>
                `).join('')}
                 <div>
                    <label class="block text-sm font-medium text-gray-400 mt-2">合計</label>
                    <div id="modal-total-score" class="mt-1 block w-full bg-zinc-900 p-2 text-right sm:text-lg font-bold">0</div>
                </div>
                 <div class="flex gap-2 pt-4">
                    <button onclick="openYakumanEventModal(${index})" class="cyber-btn text-sm px-3 py-2 rounded-md w-full"><i class="fas fa-dragon mr-2"></i>役満を追加</button>
                    <button onclick="openPenaltyModal(${index})" class="cyber-btn-red text-sm px-3 py-2 rounded-md w-full"><i class="fas fa-exclamation-triangle mr-2"></i>ペナルティを追加</button>
                </div>
            </div>
            <div class="grid grid-cols-3 gap-2">
                ${[7, 8, 9, 4, 5, 6, 1, 2, 3, 0, '00', '000'].map(key => `
                    <button onclick="keypadInput('${key}')" class="cyber-btn aspect-square text-xl md:text-2xl font-bold">${key}</button>
                `).join('')}
                <button onclick="keypadInput('マイナス')" class="cyber-btn aspect-square text-xl md:text-2xl font-bold">-</button>
                <button onclick="keypadInput('C')" class="cyber-btn-red aspect-square text-xl md:text-2xl font-bold">C</button>
                <button onclick="keypadInput('auto')" class="cyber-btn-green aspect-square text-lg md:text-xl font-bold">AUTO</button>
            </div>
        </div>
        <div class="flex justify-end gap-4 mt-6">
            <button onclick="closeModal()" class="cyber-btn px-4 py-2">キャンセル</button>
            <button onclick="saveScoresFromModal(${index})" class="cyber-btn-yellow px-4 py-2">保存</button>
        </div>
    `;
    showModal(modalHtml);
    
    setActiveInput(`modal-score-${selectedPlayers[0].id}`);
    updateModalTotal();
};

window.openYakumanEventModal = (hanchanIndex) => {
    const playerOptions = selectedPlayers.map(p => `<option value="${p.id}">${p.name}</option>`).join('');
    const yakumanCheckboxes = YAKUMAN_LIST.map(y => `
        <label class="flex items-center space-x-2">
            <input type="checkbox" name="yakuman-checkbox" value="${y}" class="form-checkbox h-5 w-5 text-blue-600 bg-gray-800 border-gray-600 rounded" onchange="window.updateYakumanCheckboxes()">
            <span>${y}</span>
        </label>
    `).join('');

    let modalContent = `
        <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">役満記録</h3>
        <div class="space-y-4">
            <div>
                <label class="block text-sm font-medium text-gray-400">雀士</label>
                <select id="yakuman-player-select" class="mt-1 block w-full rounded-md">${playerOptions}</select>
            </div>
            <div>
                <label class="block text-sm font-medium text-gray-400">役満 (複数選択可)</label>
                <div id="yakuman-checkbox-container" class="grid grid-cols-2 sm:grid-cols-3 gap-2 mt-2">${yakumanCheckboxes}</div>
            </div>
        </div>
        <div class="flex justify-end mt-6 gap-2">
            <button onclick="openScoreInputModal(${hanchanIndex})" class="cyber-btn px-4 py-2">戻る</button>
            <button onclick="addYakumanEvent(${hanchanIndex})" class="cyber-btn-green px-4 py-2">記録する</button>
        </div>
    `;
    showModal(modalContent);
};

window.updateYakumanCheckboxes = () => {
    const checkboxes = document.querySelectorAll('input[name="yakuman-checkbox"]');
    const selectedYakumans = Array.from(checkboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);

    let incompatibleYakumans = new Set();
    selectedYakumans.forEach(yakuman => {
        if (YAKUMAN_INCOMPATIBILITY[yakuman]) {
            YAKUMAN_INCOMPATIBILITY[yakuman].forEach(incompatible => {
                incompatibleYakumans.add(incompatible);
            });
        }
    });

    checkboxes.forEach(checkbox => {
        const label = checkbox.parentElement;
        if (!selectedYakumans.includes(checkbox.value) && incompatibleYakumans.has(checkbox.value)) {
            checkbox.disabled = true;
            label.classList.add('yakuman-label-disabled');
        } else {
            checkbox.disabled = false;
            label.classList.remove('yakuman-label-disabled');
        }
    });
};

window.addYakumanEvent = (hanchanIndex) => {
    const playerId = document.getElementById('yakuman-player-select').value;
    const selectedYakuman = Array.from(document.querySelectorAll('input[name="yakuman-checkbox"]:checked')).map(cb => cb.value);

    if (!playerId || selectedYakuman.length === 0) {
        showModalMessage("雀士と役満を選択してください。");
        return;
    }

    const hanchan = hanchanScores[hanchanIndex];
    if (!hanchan.yakumanEvents) {
        hanchan.yakumanEvents = [];
    }
    hanchan.yakumanEvents.push({ playerId, yakumans: selectedYakuman });
    
    showModalMessage("役満を記録しました！");
    renderScoreDisplay();
    setTimeout(() => openScoreInputModal(hanchanIndex), 1500);
};

window.openPenaltyModal = (hanchanIndex) => {
    const playerOptions = selectedPlayers.map(p => `<option value="${p.id}">${p.name}</option>`).join('');
    const chomboReasons = PENALTY_REASONS.chombo.map(r => `<option value="${r}">${r}</option>`).join('');
    const agariHoukiReasons = PENALTY_REASONS.agariHouki.map(r => `<option value="${r}">${r}</option>`).join('');

    let modalContent = `
        <h3 class="cyber-header text-xl font-bold text-red-400 mb-4">ペナルティ記録</h3>
        <div class="space-y-4">
             <div>
                <label class="block text-sm font-medium text-gray-400">雀士</label>
                <select id="penalty-player-select" class="mt-1 block w-full rounded-md">${playerOptions}</select>
            </div>
            <div>
                <label class="block text-sm font-medium text-gray-400">種類</label>
                 <select id="penalty-type-select" class="mt-1 block w-full rounded-md" onchange="updatePenaltyReasons()">
                    <option value="chombo">チョンボ</option>
                    <option value="agariHouki">アガリ放棄</option>
                </select>
            </div>
            <div>
                <label class="block text-sm font-medium text-gray-400">原因</label>
                <select id="penalty-reason-select" class="mt-1 block w-full rounded-md">${chomboReasons}</select>
            </div>
             <div>
                <label class="block text-sm font-medium text-gray-400">回数</label>
                <input type="number" id="penalty-count-input" value="1" class="mt-1 block w-full rounded-md">
            </div>
        </div>
        <div class="flex justify-end mt-6 gap-2">
            <button onclick="openScoreInputModal(${hanchanIndex})" class="cyber-btn px-4 py-2">戻る</button>
            <button onclick="addPenalty(${hanchanIndex})" class="cyber-btn-red px-4 py-2">記録する</button>
        </div>
    `;
    showModal(modalContent);
};

window.updatePenaltyReasons = () => {
    const type = document.getElementById('penalty-type-select').value;
    const reasonSelect = document.getElementById('penalty-reason-select');
    const reasons = PENALTY_REASONS[type];
    reasonSelect.innerHTML = reasons.map(r => `<option value="${r}">${r}</option>`).join('');
};

window.addPenalty = (hanchanIndex) => {
    const playerId = document.getElementById('penalty-player-select').value;
    const type = document.getElementById('penalty-type-select').value;
    const reason = document.getElementById('penalty-reason-select').value;
    const count = parseInt(document.getElementById('penalty-count-input').value, 10);

    if (!playerId || !type || !reason || isNaN(count) || count < 1) {
        showModalMessage("全ての項目を正しく入力してください。");
        return;
    }
    
    const hanchan = hanchanScores[hanchanIndex];
    if (!hanchan.penalties) {
        hanchan.penalties = [];
    }
    hanchan.penalties.push({ playerId, type, reason, count });
    
    showModalMessage("ペナルティを記録しました。");
    renderScoreDisplay();
    setTimeout(() => openScoreInputModal(hanchanIndex), 1500);
};

window.setActiveInput = (inputId) => {
    document.querySelectorAll('.modal-score-input').forEach(el => el.classList.remove('border-yellow-400', 'ring-2', 'ring-yellow-400'));
    const activeEl = document.getElementById(inputId);
    if (activeEl) {
        activeEl.classList.add('ring-2', 'ring-yellow-400');
        window.activeInputId = inputId;
    }
};

window.keypadInput = (key) => {
    const input = document.getElementById(window.activeInputId);
    if (!input) return;

    if (key === 'C') {
        input.value = '';
    } else if (key === 'マイナス') {
        if (!input.value.startsWith('-')) {
            input.value = '-' + input.value;
        }
    } else if (key === 'auto') {
        autoFillLastScore();
    } else {
        input.value += key;
    }
    updateModalTotal();
};

function updateModalTotal() {
    let total = 0;
    let filledCount = 0;
    document.querySelectorAll('.modal-score-input').forEach(input => {
        if (input.value.trim() !== '' && input.value.trim() !== '-') {
            total += Number(input.value);
            filledCount++;
        }
    });
    const totalDiv = document.getElementById('modal-total-score');
    const basePoint = Number(document.getElementById('base-point').value);
    totalDiv.textContent = total.toLocaleString();
    totalDiv.classList.remove('text-red-500', 'text-green-400');
    if (filledCount === 4) {
        if(total === basePoint * 4) {
            totalDiv.classList.add('text-green-400');
        } else {
            totalDiv.classList.add('text-red-500');
        }
    }
}

function autoFillLastScore() {
    let total = 0;
    let emptyInput = null;
    let filledCount = 0;
    document.querySelectorAll('.modal-score-input').forEach(input => {
        if (input.value.trim() !== '' && input.value.trim() !== '-') {
            total += Number(input.value);
            filledCount++;
        } else {
            emptyInput = input;
        }
    });

    if (filledCount === 3 && emptyInput) {
        const basePoint = Number(document.getElementById('base-point').value);
        const targetTotal = basePoint * 4;
        emptyInput.value = targetTotal - total;
        updateModalTotal();
    }
}

window.saveScoresFromModal = (index) => {
    const newScores = {};
    let total = 0;
    let hasEmpty = false;
    selectedPlayers.forEach(p => {
        const input = document.getElementById(`modal-score-${p.id}`);
        const value = input.value;
        if (value === '' || value === null || value === '-') {
            hasEmpty = true;
            newScores[p.id] = null;
        } else {
            const score = Number(value);
            newScores[p.id] = score;
            total += score;
        }
    });

    if (!hasEmpty) {
        const basePoint = Number(document.getElementById('base-point').value);
        if (Math.round(total) !== basePoint * 4) {
            showModalMessage(`合計点が ${basePoint*4} になっていません。(現在: ${total})`);
            return;
        }
    }
    
    hanchanScores[index].rawScores = newScores;
    renderScoreDisplay();
    closeModal();
};

function getGameDataFromForm(onlyCompleted) {
    const basePoint = Number(document.getElementById('base-point').value);
    const returnPoint = Number(document.getElementById('return-point').value);
    const uma = [
        Number(document.getElementById('uma-1').value), Number(document.getElementById('uma-2').value),
        Number(document.getElementById('uma-3').value), Number(document.getElementById('uma-4').value)
    ];
    
    const processedHanchans = [];
    
    for (let i = 0; i < hanchanScores.length; i++) {
        const hanchan = hanchanScores[i];
        const scores = hanchan.rawScores;
        const isComplete = Object.values(scores).every(s => s !== null && s !== '');
        
        if (!isComplete) {
            if (onlyCompleted) continue;
            return { error: `半荘 #${i + 1} の全ての素点を入力してください。` };
        }

        const total = Object.values(scores).reduce((sum, score) => sum + Number(score), 0);
        if (Math.round(total) !== basePoint * 4) {
            return { error: `半荘 #${i + 1} の合計点が ${basePoint*4} になっていません。(現在: ${total})` };
        }
        processedHanchans.push(hanchan);
    }

    if (processedHanchans.length === 0 && onlyCompleted) {
        return { error: "ポイント計算できる完成した半荘がありません。" };
    }

    const totalPoints = {};
    selectedPlayers.forEach(p => totalPoints[p.id] = 0);

    processedHanchans.forEach(hanchan => {
        const { points } = calculateHanchanRanksAndPoints(hanchan.rawScores);
        hanchan.points = points;
        Object.keys(points).forEach(playerId => {
            totalPoints[playerId] += points[playerId];
        });
    });

    return { hanchanData: processedHanchans, totalPoints, settings: { basePoint, returnPoint, uma } };
}

window.savePartialData = () => {
    const gameData = {
        selectedPlayers: selectedPlayers,
        gameDate: document.getElementById('game-date').value,
        basePoint: document.getElementById('base-point').value,
        returnPoint: document.getElementById('return-point').value,
        uma1: document.getElementById('uma-1').value,
        uma2: document.getElementById('uma-2').value,
        uma3: document.getElementById('uma-3').value,
        uma4: document.getElementById('uma-4').value,
        scores: hanchanScores
    };
    localStorage.setItem('edogawa-m-league-partial', JSON.stringify(gameData));
    showModalMessage("途中データを保存しました！");
};

function loadSavedGameData() {
    const savedData = localStorage.getItem('edogawa-m-league-partial');
    if (savedData) {
        const data = JSON.parse(savedData);
        if (data.selectedPlayers && data.selectedPlayers.length === 4) {
            selectedPlayers = data.selectedPlayers;
            renderPlayerSelection();
            document.getElementById('to-step2-btn').disabled = false;
            
            document.getElementById('step2-rule-settings').classList.remove('hidden');
            document.getElementById('step3-score-input').classList.remove('hidden');
            
            document.getElementById('base-point').value = data.basePoint;
            document.getElementById('return-point').value = data.returnPoint;
            document.getElementById('uma-1').value = data.uma1;
            document.getElementById('uma-2').value = data.uma2;
            document.getElementById('uma-3').value = data.uma3;
            document.getElementById('uma-4').value = data.uma4;
            updateOkaDisplay();
            
            lockUnlockStep(1, true);
            lockUnlockStep(2, true);

            hanchanScores = data.scores || [];
            if (hanchanScores.length === 0) {
                addHanchan();
            } else {
                renderScoreDisplay();
            }

            showModalMessage("保存されたデータを読み込みました。");
        }
    }
}

window.showCurrentPtStatus = () => {
    const gameData = getGameDataFromForm(true); // only completed
    if (gameData.error) {
        showModalMessage(gameData.error);
        return;
    }

    const rankedPlayers = Object.entries(gameData.totalPoints)
        .map(([id, points]) => ({ id, name: selectedPlayers.find(p=>p.id===id).name, points }))
        .sort((a, b) => b.points - a.points);
    
    let modalHtml = `<h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">現在のポイント状況</h3>
        <table class="min-w-full text-center">
            <thead><tr class="border-b border-gray-700"><th class="py-2">順位</th><th class="py-2">雀士</th><th class="py-2">ポイント</th></tr></thead><tbody>`;
    
    rankedPlayers.forEach((player, index) => {
        modalHtml += `<tr>
            <td class="py-2">${index + 1}</td>
            <td class="py-2">${player.name}</td>
            <td class="py-2 font-bold ${player.points >= 0 ? 'text-green-400' : 'text-red-400'}">${player.points.toFixed(1)}</td>
        </tr>`;
    });

    modalHtml += `</tbody></table><div class="flex justify-end mt-6"><button onclick="closeModal()" class="cyber-btn px-4 py-2">閉じる</button></div>`;
    showModal(modalHtml);
};

window.calculateAndSave = async () => {
    const gameDate = document.getElementById('game-date').value.trim();
    if (!gameDate) {
        showModalMessage("対局日を入力してください。");
        return;
    }

    const gameData = getGameDataFromForm(false);
    if (gameData.error) {
        showModalMessage(gameData.error);
        return;
    }
    
    const dataToSave = {
        gameDate: gameDate,
        playerIds: selectedPlayers.map(p => p.id), 
        playerNames: selectedPlayers.map(p => p.name),
        settings: gameData.settings, 
        scores: gameData.hanchanData, 
        totalPoints: gameData.totalPoints
    };

    try {
        if (editingGameId) {
            // Update existing game
            const gameDocRef = doc(db, 'games', editingGameId);
            await updateDoc(gameDocRef, dataToSave);
            showModalMessage("対局結果を更新しました！");
        } else {
            // Add new game
            dataToSave.createdAt = new Date();
            await addDoc(collection(db, `games`), dataToSave);
            showModalMessage("対局結果を保存しました！");
        }
        resetGame();
        localStorage.removeItem('edogawa-m-league-partial');
    } catch (error) { 
        console.error("Error saving game: ", error); 
        showModalMessage("データの保存に失敗しました。"); 
    }
};

function showModal(content) {
    document.getElementById('modal-content').innerHTML = content;
    document.getElementById('modal').classList.remove('hidden');
}
function showModalMessage(message) {
    showModal(`<p class="text-lg text-center">${message}</p><div class="flex justify-end mt-6"><button onclick="closeModal()" class="cyber-btn px-6 py-2">閉じる</button></div>`);
}
function showLoadingModal(message) {
    showModal(`<div class="flex justify-center items-center flex-col gap-4"><p class="text-lg text-center">${message}</p><div class="animate-spin rounded-full h-8 w-8 border-b-2 border-yellow-400"></div></div>`);
}
window.closeModal = () => document.getElementById('modal').classList.add('hidden');

window.showGameDetails = (gameId) => {
    const game = games.find(g => g.id === gameId);
    if (!game) return;
    const date = game.gameDate || new Date(game.createdAt.seconds * 1000).toLocaleString('ja-JP');
    
    const playerHeaders = game.playerIds.map(pId => {
        const player = users.find(u => u.id === pId);
        const photoHtml = getPlayerPhotoHtml(pId, 'w-10 h-10');
        return `<th class="px-2 py-2 text-center">
            ${photoHtml}
            <div class="text-xs mt-1">${player ? player.name : 'N/A'}</div>
        </th>`;
    }).join('');
    
    const ranksBody = game.scores.map((hanchan, index) => {
        const scoreGroups = {};
        Object.entries(hanchan.rawScores).forEach(([pId, score]) => {
            if (!scoreGroups[score]) scoreGroups[score] = [];
            scoreGroups[score].push(pId);
        });
        const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
        
        const ranks = {};
        let rankCursor = 1;
        sortedScores.forEach(score => {
            const playersInGroup = scoreGroups[score];
            playersInGroup.forEach(pId => {
                ranks[pId] = rankCursor;
            });
            rankCursor += playersInGroup.length;
        });

        const rankCells = game.playerIds.map(pId => {
            const rank = ranks[pId];
            let rankClass = '';
            if (rank === 1) rankClass = 'text-rank-1';
            if (rank === 4) rankClass = 'text-rank-4';
            return `<td class="px-2 py-2 text-center font-m-gothic ${rankClass}">${rank}</td>`;
        }).join('');
        return `<tr><td class="px-2 py-2 text-center font-bold">#${index + 1}</td>${rankCells}</tr>`;
    }).join('');

    const createTableBody = (dataType) => {
        return game.scores.map((hanchan, index) => {
            const scores = hanchan[dataType];
            const scoreGroups = {};
            Object.entries(hanchan.rawScores).forEach(([pId, score]) => {
                if (!scoreGroups[score]) scoreGroups[score] = [];
                scoreGroups[score].push(pId);
            });
            const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
            
            const ranks = {};
            let rankCursor = 1;
            sortedScores.forEach(score => {
                const playersInGroup = scoreGroups[score];
                playersInGroup.forEach(pId => {
                    ranks[pId] = rankCursor;
                });
                rankCursor += playersInGroup.length;
            });

            const scoreCells = game.playerIds.map(pId => {
                const score = scores[pId];
                const rank = ranks[pId];
                let rankClass = '';
                if (rank === 1) rankClass = 'text-rank-1';
                if (rank === 4) rankClass = 'text-rank-4';
                return `<td class="px-2 py-2 text-center font-m-gothic ${rankClass}">${dataType === 'points' ? score.toFixed(1) : score.toLocaleString()}</td>`;
            }).join('');
            return `<tr><td class="px-2 py-2 text-center font-bold">#${index + 1}</td>${scoreCells}</tr>`;
        }).join('');
    };

    const pointsBody = createTableBody('points');
    const rawScoresBody = createTableBody('rawScores');

    let detailsHtml = `
        <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-2">${date}</h3>
        <div class="space-y-6">
            <div>
                <h4 class="font-bold mb-2">着順</h4>
                <div class="overflow-x-auto"><table class="min-w-full text-sm"><thead><tr><th class="px-2 py-2 text-center">#</th>${playerHeaders}</tr></thead><tbody class="divide-y divide-gray-700">${ranksBody}</tbody></table></div>
            </div>
            <div>
                <h4 class="font-bold mb-2">ポイント</h4>
                <div class="overflow-x-auto"><table class="min-w-full text-sm"><thead><tr><th class="px-2 py-2 text-center">#</th>${playerHeaders}</tr></thead><tbody class="divide-y divide-gray-700">${pointsBody}</tbody></table></div>
            </div>
            <div>
                <h4 class="font-bold mb-2">素点</h4>
                <div class="overflow-x-auto"><table class="min-w-full text-sm"><thead><tr><th class="px-2 py-2 text-center">#</th>${playerHeaders}</tr></thead><tbody class="divide-y divide-gray-700">${rawScoresBody}</tbody></table></div>
            </div>
        </div>
        <div class="flex justify-end mt-6"><button onclick="closeModal()" class="cyber-btn px-4 py-2">閉じる</button></div>
    `;
    showModal(detailsHtml);
};

function lockUnlockStep(stepNum, lock) {
    const stepDiv = document.getElementById(`step${stepNum}-player-selection`) || document.getElementById(`step${stepNum}-rule-settings`);
    if (!stepDiv) return;
    const elements = stepDiv.querySelectorAll('input, button, select');
    elements.forEach(el => {
        // Allow navigation buttons to be enabled/disabled separately
        if (!el.id.includes('to-step') && !el.id.includes('back-to-step')) {
            el.disabled = lock;
        }
    });
}

window.moveToStep2 = () => {
    lockUnlockStep(1, true);
    const step2 = document.getElementById('step2-rule-settings');
    step2.classList.remove('hidden');
    step2.scrollIntoView({ behavior: 'smooth', block: 'center' });
};

window.moveToStep3 = () => {
    lockUnlockStep(2, true);
    const step3 = document.getElementById('step3-score-input');
    step3.classList.remove('hidden');
    if (hanchanScores.length === 0) {
        setupScoreDisplay();
    }
    step3.scrollIntoView({ behavior: 'smooth', block: 'center' });
};

window.backToStep1 = () => {
    if (editingGameId) return; // Don't allow going back if editing
    document.getElementById('step2-rule-settings').classList.add('hidden');
    lockUnlockStep(1, false);
    document.getElementById('step1-player-selection').scrollIntoView({ behavior: 'smooth', block: 'center' });
    localStorage.removeItem('edogawa-m-league-partial');
};

window.backToStep2 = () => {
    if (editingGameId) return; // Don't allow going back if editing
    document.getElementById('step3-score-input').classList.add('hidden');
    lockUnlockStep(2, false);
    document.getElementById('step2-rule-settings').scrollIntoView({ behavior: 'smooth', block: 'center' });
};

const setupRuleEventListeners = () => {
    document.getElementById('base-point')?.addEventListener('input', updateOkaDisplay);
    document.getElementById('return-point')?.addEventListener('input', updateOkaDisplay);
};
const updateOkaDisplay = () => {
     const base = Number(document.getElementById('base-point')?.value || 0);
     const ret = Number(document.getElementById('return-point')?.value || 0);
     const oka = ((ret - base) * 4) / 1000;
     const display = document.getElementById('oka-display');
     if(display) display.textContent = oka.toFixed(1);
};
window.setMLeagueRules = () => {
    document.getElementById('base-point').value = 25000; document.getElementById('return-point').value = 30000;
    document.getElementById('uma-1').value = 30; document.getElementById('uma-2').value = 10;
    document.getElementById('uma-3').value = -10; document.getElementById('uma-4').value = -30;
    updateOkaDisplay();
};

window.setTodayDate = () => {
    const today = new Date();
    const year = today.getFullYear();
    const month = today.getMonth() + 1;
    const day = today.getDate();
    const dayOfWeek = ['日', '月', '火', '水', '木', '金', '土'][today.getDay()];
    const formattedDate = `${year}/${month}/${day}(${dayOfWeek})`;
    document.getElementById('game-date').value = formattedDate;
};

window.togglePlayerSelection = (checkbox) => {
    const user = users.find(u => u.id === checkbox.value);
    if (!user) return;

    if (checkbox.checked) { 
        if (selectedPlayers.length < 4) {
            selectedPlayers.push({ id: user.id, name: user.name, photoURL: user.photoURL });
        }
    }
    else { 
        selectedPlayers = selectedPlayers.filter(p => p.id !== checkbox.value); 
    }
    document.getElementById('to-step2-btn').disabled = selectedPlayers.length !== 4;
    renderPlayerSelection();
};

const resetGame = () => {
    selectedPlayers = [];
    hanchanScores = [];
    editingGameId = null;
    renderGameTab();
    
    const saveBtn = document.getElementById('save-game-btn');
    if (saveBtn) {
        saveBtn.innerHTML = `<i class="fas fa-save mr-2"></i>Pt変換して保存`;
        saveBtn.classList.remove('cyber-btn-green');
        saveBtn.classList.add('cyber-btn-yellow');
    }
    
    changeTab('game');
};

window.updateHeadToHeadDropdowns = () => {
    const p1Select = document.getElementById('h2h-player1');
    const p2Select = document.getElementById('h2h-player2');
    if (!p1Select || !p2Select) return;

    const currentP1 = p1Select.value;
    const currentP2 = p2Select.value;
    
    const options = users.map(u => `<option value="${u.id}">${u.name}</option>`).join('');
    
    p1Select.innerHTML = options;
    p2Select.innerHTML = options;

    p1Select.value = currentP1 || (users.length > 0 ? users[0].id : '');
    p2Select.value = currentP2 || (users.length > 1 ? users[1].id : '');
};

window.displayHeadToHeadStats = () => {
    updateHeadToHeadDropdowns();
    const p1Id = document.getElementById('h2h-player1')?.value;
    const p2Id = document.getElementById('h2h-player2')?.value;
    const resultsContainer = document.getElementById('h2h-results');

    if (!p1Id || !p2Id) {
        resultsContainer.innerHTML = `<p class="text-gray-500">比較したい雀士を2名選択してください。</p>`;
        return;
    }
    if (p1Id === p2Id) {
        resultsContainer.innerHTML = `<p class="text-yellow-400">同じ雀士は選択できません。</p>`;
        return;
    }

    const p1 = users.find(u => u.id === p1Id);
    const p2 = users.find(u => u.id === p2Id);

    let p1Wins = 0;
    let p2Wins = 0;
    let draws = 0;
    let totalHanchans = 0;

    games.forEach(game => {
        if (game.playerIds.includes(p1Id) && game.playerIds.includes(p2Id)) {
            game.scores.forEach(hanchan => {
                totalHanchans++;
                const p1Score = hanchan.rawScores[p1Id];
                const p2Score = hanchan.rawScores[p2Id];
                if (p1Score > p2Score) {
                    p1Wins++;
                } else if (p2Score > p1Score) {
                    p2Wins++;
                } else {
                    draws++;
                }
            });
        }
    });

    if (totalHanchans === 0) {
        resultsContainer.innerHTML = `<p class="text-gray-500">この2人の直接対決の記録はありません。</p>`;
        return;
    }
    
    const p1WinRate = ((p1Wins / totalHanchans) * 100).toFixed(1);
    const p2WinRate = ((p2Wins / totalHanchans) * 100).toFixed(1);

    resultsContainer.innerHTML = `
        <div class="text-2xl mb-4">総対戦半荘数: <span class="font-bold text-yellow-400">${totalHanchans}</span>回</div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 items-center">
            <div class="cyber-card p-4 text-center">
                ${getPlayerPhotoHtml(p1.id, 'w-20 h-20 mx-auto mb-2')}
                <h3 class="text-2xl font-bold text-blue-400">${p1.name}</h3>
                <p class="text-4xl font-bold text-green-400 my-2">${p1Wins}<span class="text-xl">勝</span></p>
                <p class="text-gray-400">勝率: ${p1WinRate}%</p>
            </div>
            <div class="cyber-card p-4 text-center">
                ${getPlayerPhotoHtml(p2.id, 'w-20 h-20 mx-auto mb-2')}
                <h3 class="text-2xl font-bold text-blue-400">${p2.name}</h3>
                <p class="text-4xl font-bold text-green-400 my-2">${p2Wins}<span class="text-xl">勝</span></p>
                <p class="text-gray-400">勝率: ${p2WinRate}%</p>
            </div>
        </div>
        ${draws > 0 ? `<p class="mt-4 text-gray-500">引き分け: ${draws}回</p>` : ''}
    `;
};

// --- Trophy Functions ---
const TROPHY_DEFINITIONS = {
    bronze: [
        { id: 'first_game', name: '初陣', desc: '初めて対局に参加する', icon: 'fa-chess-pawn' },
        { id: 'first_top', name: '初トップ', desc: '初めてトップを取る', icon: 'fa-crown' },
        { id: 'first_plus_day', name: '初勝利の味', desc: '初めて1日の収支をプラスで終える', icon: 'fa-arrow-trend-up' },
        { id: 'ten_games', name: '雀士認定', desc: '累計10半荘を戦い抜く', icon: 'fa-user-check' },
        { id: 'first_busted', name: 'これも経験', desc: '初めてトんだ', icon: 'fa-piggy-bank' },
        { id: 'first_last', name: '捲土重来の誓い', desc: '初めて4位で半荘を終えた', icon: 'fa-flag' },
        { id: 'score_under_1000', name: '虫の息', desc: '持ち点が1,000点未満の状態で対局を終える', icon: 'fa-heart-pulse' },
        { id: 'daily_high_score', name: '今日のヒーロー', desc: '1日の対局で最も高いスコアを記録する', icon: 'fa-star' },
    ],
    silver: [
        { id: 'twenty_five_games', name: 'リーグの主軸', desc: '累計25半荘を戦い抜く', icon: 'fa-users' },
        { id: 'yakuman', name: '神域の淵', desc: '初めて役満を和了する', icon: 'fa-dragon' },
        { id: 'plus_100_day', name: '爆勝ち', desc: '1日で合計+100pt以上を獲得する', icon: 'fa-sack-dollar' },
        { id: 'five_rentai', name: '連対の鬼', desc: '5連続で連対(1位か2位)を達成する', icon: 'fa-link' },
        { id: 'score_over_50k', name: '高打点メーカー', desc: '1半荘で50,000点以上を獲得する', icon: 'fa-bomb' },
        { id: 'dramatic_finish', name: 'ドラマティック・フィニッシュ', desc: '1日の最終半荘で逆転して1位になる', icon: 'fa-film' },
        { id: 'ten_tops', name: 'トップハンター', desc: '通算1位獲得回数が10回に到達する', icon: 'fa-crosshairs' },
        { id: 'monthly_player', name: 'マンスリープレイヤー', desc: '1か月のうちに15半荘以上対局する', icon: 'fa-calendar-days' },
        { id: 'zero_point_finish', name: '実質何もしてない', desc: '収支が±0.0で対局を終える', icon: 'fa-equals' },
    ],
    gold: [
        { id: 'fifty_tops', name: '伝説の始まり', desc: '通算1位獲得回数が50回に到達する', icon: 'fa-book-journal-whills' },
        { id: 'self_redemption', name: '自らの尻拭い', desc: '前回の対局のマイナス収支を、今回のプラス収支で完全に取り返す', icon: 'fa-hand-sparkles' },
        { id: 'close_win', name: 'ハナ差', desc: '2位と1,000点未満の点差で1位になる', icon: 'fa-ruler-horizontal' },
        { id: 'all_negative_win', name: 'やりたい事やりすぎ', desc: 'その半荘の参加者全員をマイナス収支にして1位を取る', icon: 'fa-volcano' },
        { id: 'ten_no_last', name: 'カッチカチ麻雀', desc: '10半荘連続でラス回避をする', icon: 'fa-shield-virus' },
        { id: 'three_same_rank', name: 'マイブーム？', desc: '3半荘連続で同じ順位を取り続ける', icon: 'fa-clone' },
        { id: 'finish_over_50k', name: '勝者の余裕', desc: '1回の対局で50,000点以上を獲得して終了する', icon: 'fa-champagne-glasses' },
        { id: 'score_under_minus_30k', name: '地底の奥底', desc: '-30,000点未満で対局を終える', icon: 'fa-person-falling-burst' },
    ],
    platinum: [
        { id: 'two_hundred_games', name: '君がいなきゃ始まらない', desc: '累計200半荘を戦い抜く', icon: 'fa-book-skull' },
        { id: 'four_top_streak', name: '雀神の導き', desc: '4半荘連続で1位を獲得する', icon: 'fa-brain' },
        { id: 'twenty_five_no_last', name: '絶対防衛線', desc: '25回連続でラスを回避する', icon: 'fa-torii-gate' },
        { id: 'finish_over_70k', name: '背中も見せない', desc: '70,000点以上を獲得して対局を終える', icon: 'fa-person-running' },
        { id: 'avg_rank_2_3', name: '圧倒的実力', desc: '年間平均順位が2.3以下（年間50半荘以上）', icon: 'fa-chart-line' },
        { id: 'ten_close_games', name: '歴戦の猛者', desc: '1,000点差以内の僅差での決着を10回経験する（勝ち負け問わず）', icon: 'fa-swords' },
        { id: 'undefeated_month', name: '無敗神話', desc: '1ヶ月間（月間10半荘以上）、一度も4位を取らない', icon: 'fa-calendar-check' },
        { id: 'kokushi', name: '十三の旗印', desc: '国士無双を和了する', icon: 'fa-flag', secret: true },
        { id: 'suuankou', name: '闇に潜む刺客', desc: '四暗刻を和了する', icon: 'fa-user-secret', secret: true },
        { id: 'daisangen', name: '三元龍の咆哮', desc: '大三元を和了する', icon: 'fa-dragon', secret: true },
        { id: 'tsuuiisou', name: '刻まれし言霊', desc: '字一色を和了する', icon: 'fa-font', secret: true },
        { id: 'ryuuiisou', name: '翠玉の輝き', desc: '緑一色を和了する', icon: 'fa-leaf', secret: true },
        { id: 'chinroutou', name: '万物の始祖', desc: '清老頭を和了する', icon: 'fa-dice-one', secret: true },
        { id: 'chuuren', name: '九連の灯火', desc: '九蓮宝燈を和了する', icon: 'fa-lightbulb', secret: true },
        { id: 'shousuushii', name: '風の支配者', desc: '小四喜を和了する', icon: 'fa-wind', secret: true },
    ],
    crystal: [
        { id: 'five_top_streak', name: '天衣無縫', desc: '5半荘連続で1位を獲得する', icon: 'fa-feather-pointed' },
        { id: 'yearly_avg_rank_2_0', name: '頂への道', desc: '年間平均順位が2.0以下（年間50戦以上）', icon: 'fa-mountain-sun' },
        { id: 'recent_100_avg_rank_1_5', name: '全知全能', desc: '直近100半荘の平均着順が1.5以下', icon: 'fa-eye' },
        { id: 'thirty_no_last', name: 'アルティメット・ガーディアン', desc: '30半荘連続でラスを回避する', icon: 'fa-shield-heart' },
        { id: 'finish_over_100k', name: '膏血の強奪', desc: '100,000点以上を獲得して対局を終える', icon: 'fa-gem' },
        { id: 'two_yakuman_day', name: '神はサイコロを振らない', desc: '1日のうちに2回役満を和了する', icon: 'fa-dice' },
        { id: 'three_yakuman_types', name: '役満コレクター', desc: '3種類以上の役満を和了する', icon: 'fa-box-archive' },
        { id: 'tenhou', name: '天命', desc: '天和を和了する', icon: 'fa-hand-sparkles', secret: true },
        { id: 'chiihou', name: '地の啓示', desc: '地和を和了する', icon: 'fa-hand-holding-hand', secret: true },
        { id: 'kokushi13', name: '終焉の十三面', desc: '国士無双十三面待ちを和了する', icon: 'fa-skull', secret: true },
        { id: 'suuankou_tanki', name: '静寂切り裂く一閃', desc: '四暗刻単騎を和了する', icon: 'fa-user-ninja', secret: true },
        { id: 'junsei_chuuren', name: '死の篝火', desc: '純正九蓮宝燈を和了する', icon: 'fa-fire', secret: true },
        { id: 'daisuushii', name: '風を統べる者', desc: '大四喜を和了する', icon: 'fa-tornado', secret: true },
    ],
    chaos: [
        { id: 'yakuman_then_busted_last', name: '天国と地獄', desc: '役満を和了した次の対局で、ハコテンラスになる', icon: 'fa-yin-yang' },
        { id: 'perfect_world', name: '完全世界', desc: '4人の最終スコアが、1位から順に40,000、30,000、20,000、10,000点になる', icon: 'fa-globe' },
        { id: 'reincarnation', name: '輪廻転生', desc: '4人の順位が、前回の対局の順位から完全に逆転する', icon: 'fa-recycle' },
        { id: 'reroll', name: 'リセマラ', desc: '最初の持ち点と全く同じ点数で対局を終える', icon: 'fa-arrow-rotate-left' },
        { id: 'chaos_theory', name: 'カオス理論', desc: '4半荘連続で、4人全員が違う順位になる', icon: 'fa-hurricane' },
        { id: 'peaceful_village', name: '平和村', desc: '4人の最終スコアが、全員25,000点ずつになる', icon: 'fa-dove' },
    ]
};

function renderTrophyTab() {
    const container = document.getElementById('trophy-tab');
    if (!container) return;
    container.innerHTML = `
        <div class="flex flex-col sm:flex-row justify-between items-center mb-6 gap-4">
            <h2 class="cyber-header text-2xl font-bold text-blue-400">トロフィー</h2>
            <div class="flex items-center gap-4">
                <select id="trophy-year-filter" onchange="window.updateTrophyPage()" class="rounded-md p-1"></select>
                <select id="trophy-player-filter" onchange="window.updateTrophyPage()" class="rounded-md p-1"></select>
            </div>
        </div>
        <div id="trophy-list-container" class="space-y-8"></div>
    `;
}

window.updateTrophyPage = function() {
    const container = document.getElementById('trophy-list-container');
    const yearSelect = document.getElementById('trophy-year-filter');
    const playerSelect = document.getElementById('trophy-player-filter');

    if (!container || !yearSelect || !playerSelect) return;

    // Preserve current selection before repopulating
    const currentYear = yearSelect.value;
    const currentPlayer = playerSelect.value;

    // Regenerate options
    const yearOptions = getGameYears().map(year => `<option value="${year}" ${currentYear === year ? 'selected' : ''}>${year}年</option>`).join('');
    const playerOptions = users.map(u => `<option value="${u.id}" ${currentPlayer === u.id ? 'selected' : ''}>${u.name}</option>`).join('');

    // Update select elements' innerHTML
    yearSelect.innerHTML = `<option value="all" ${currentYear === 'all' || !currentYear ? 'selected' : ''}>全期間</option>${yearOptions}`;
    playerSelect.innerHTML = `<option value="all" ${currentPlayer === 'all' || !currentPlayer ? 'selected' : ''}>全選択</option>${playerOptions}`;

    // Get the final filter values
    const yearFilter = yearSelect.value;
    const playerFilter = playerSelect.value;

    // Filter games based on the selected year
    const filteredGames = games.filter(game => {
        if (yearFilter === 'all') return true;
        const gameYear = (game.gameDate || '').substring(0, 4);
        return gameYear === yearFilter;
    });
    
    // Calculate stats for the filtered period
    const statsForPeriod = calculateAllPlayerStats(filteredGames);

    // Check which trophies have been achieved based on filtered data
    checkAllTrophies(filteredGames, statsForPeriod);

    // Render the trophy list
    let html = '';
    Object.entries(TROPHY_DEFINITIONS).forEach(([rank, trophies]) => {
        html += `<div class="rank-category">
            <h3 class="cyber-header text-xl font-bold mb-4 border-b-2 pb-2" style="border-color: var(--rank-${rank}); color: var(--rank-${rank});">${rank.charAt(0).toUpperCase() + rank.slice(1)}</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                ${trophies.map(trophy => {
                    let isAchieved = false;
                    // Check achievement based on the player filter
                    if (playerFilter === 'all') {
                        isAchieved = Object.values(playerTrophies).some(p => p[trophy.id]);
                    } else {
                        isAchieved = playerTrophies[playerFilter]?.[trophy.id] || false;
                    }
                    const secretClass = trophy.secret ? 'secret' : '';
                    const trophyName = (trophy.secret && !isAchieved) ? '？？？' : trophy.name;
                    const trophyDesc = (trophy.secret && !isAchieved) ? '条件を満たすと開示されます' : trophy.desc;
                    const trophyIcon = (trophy.secret && !isAchieved) ? 'fa-question-circle' : trophy.icon;

                    return `
                    <div class="trophy-card p-4 flex items-center gap-4 rounded-lg rank-${rank} ${isAchieved ? 'achieved' : ''} ${secretClass}">
                        <i class="fas ${trophyIcon} fa-3x w-12 text-center trophy-icon"></i>
                        <div>
                            <h4 class="font-bold text-lg">${trophyName}</h4>
                            <p class="text-sm text-gray-400">${trophyDesc}</p>
                        </div>
                    </div>
                    `;
                }).join('')}
            </div>
        </div>`;
    });
    container.innerHTML = html;
}

function checkAllTrophies(targetGames, currentStats) {
    playerTrophies = {};
    const rankedUsers = Object.values(currentStats).filter(u => u.totalHanchans > 0).sort((a,b) => b.totalPoints - a.totalPoints);

    users.forEach(user => {
        playerTrophies[user.id] = {};
        const stats = currentStats[user.id];
        if (!stats || stats.totalHanchans === 0) return;

        const playerGames = targetGames.filter(g => g.playerIds.includes(user.id));
        const dailyPoints = {};
        const dailyScores = {};
        const monthlyHanchans = {};

        playerGames.forEach(g => {
            if (g.gameDate) {
                const date = g.gameDate.split('(')[0];
                if(!dailyPoints[date]) dailyPoints[date] = 0;
                dailyPoints[date] += g.totalPoints[user.id];
                
                if(!dailyScores[date]) dailyScores[date] = [];
                g.scores.forEach(s => dailyScores[date].push(s.rawScores[user.id]));

                const month = g.gameDate.substring(0, 7); // yyyy/mm
                if(!monthlyHanchans[month]) monthlyHanchans[month] = 0;
                monthlyHanchans[month] += g.scores.length;

            }
        });
        
        // Bronze
        playerTrophies[user.id].first_game = stats.totalHanchans > 0;
        playerTrophies[user.id].first_top = stats.ranks[0] > 0;
        playerTrophies[user.id].first_plus_day = Object.values(dailyPoints).some(p => p > 0);
        playerTrophies[user.id].ten_games = stats.totalHanchans >= 10;
        playerTrophies[user.id].first_busted = stats.bustedCount > 0;
        playerTrophies[user.id].first_last = stats.ranks[3] > 0;
        playerTrophies[user.id].score_under_1000 = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] < 1000 && s.rawScores[user.id] >= 0));
        playerTrophies[user.id].daily_high_score = Object.keys(dailyPoints).some(date => {
            let maxScoreToday = -Infinity;
            targetGames.filter(g => g.gameDate && g.gameDate.startsWith(date)).forEach(g => {
                g.scores.forEach(s => {
                    Object.values(s.rawScores).forEach(score => {
                        if(score > maxScoreToday) maxScoreToday = score;
                    })
                })
            });
            return dailyScores[date] && dailyScores[date].some(s => s === maxScoreToday);
        });

        // Silver
        playerTrophies[user.id].twenty_five_games = stats.totalHanchans >= 25;
        playerTrophies[user.id].yakuman = stats.yakumanCount > 0;
        playerTrophies[user.id].plus_100_day = Object.values(dailyPoints).some(p => p >= 100);
        playerTrophies[user.id].five_rentai = stats.maxStreak.rentai >= 5;
        playerTrophies[user.id].score_over_50k = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] >= 50000));
        playerTrophies[user.id].ten_tops = stats.ranks[0] >= 10;
        playerTrophies[user.id].monthly_player = Object.values(monthlyHanchans).some(count => count >= 15);
        playerTrophies[user.id].zero_point_finish = playerGames.some(g => g.totalPoints[user.id] === 0.0);
        
        const allDailyPoints = {};
        targetGames.forEach(g => {
            if (g.gameDate) {
                const date = g.gameDate.split('(')[0];
                if (!allDailyPoints[date]) {
                    allDailyPoints[date] = {};
                }
                Object.entries(g.totalPoints).forEach(([pId, points]) => {
                    if (!allDailyPoints[date][pId]) {
                        allDailyPoints[date][pId] = 0;
                    }
                    allDailyPoints[date][pId] += points;
                });
            }
        });

        playerTrophies[user.id].dramatic_finish = Object.keys(dailyPoints).some(date => {
            const gamesOnDate = playerGames.filter(g => g.gameDate && g.gameDate.startsWith(date)).sort((a,b) => (a.createdAt?.seconds || 0) - (b.createdAt?.seconds || 0));
            if (gamesOnDate.length < 2) return false;

            const dailyTotalsForDate = allDailyPoints[date];
            if (!dailyTotalsForDate || Object.keys(dailyTotalsForDate).length === 0) return false;
            
            const winnerEntry = Object.entries(dailyTotalsForDate).reduce((a, b) => (a[1] > b[1] ? a : b), [null, -Infinity]);
            const dailyWinnerId = winnerEntry[0];

            if (dailyWinnerId !== user.id) return false;

            const lastGame = gamesOnDate[gamesOnDate.length - 1];
            
            const pointsBeforeLastGame = {};
            Object.entries(dailyTotalsForDate).forEach(([pId, total]) => {
                const pointsInLast = lastGame.totalPoints[pId] || 0;
                pointsBeforeLastGame[pId] = total - pointsInLast;
            });

            if (Object.keys(pointsBeforeLastGame).length === 0) return false;

            const winnerBeforeLastGameEntry = Object.entries(pointsBeforeLastGame).reduce((a, b) => (a[1] > b[1] ? a : b), [null, -Infinity]);
            const winnerBeforeLastGameId = winnerBeforeLastGameEntry[0];
            
            return winnerBeforeLastGameId !== user.id;
        });
        
        // Gold
        playerTrophies[user.id].fifty_tops = stats.ranks[0] >= 50;
        playerTrophies[user.id].close_win = playerGames.some(g => g.scores.some(s => {
            const scores = Object.entries(s.rawScores).sort((a,b) => b[1] - a[1]);
            return scores.length > 1 && scores[0][0] === user.id && (scores[0][1] - scores[1][1]) < 1000;
        }));
        playerTrophies[user.id].all_negative_win = playerGames.some(g => g.scores.some(s => {
            const scores = Object.entries(s.rawScores);
            const myScore = scores.find(([pId]) => pId === user.id)?.[1] || 0;
            return myScore > 0 && scores.filter(([pId]) => pId !== user.id).every(([, score]) => score < 0);
        }));
        playerTrophies[user.id].ten_no_last = stats.maxStreak.noLast >= 10;
        playerTrophies[user.id].three_same_rank = stats.maxStreak.sameRank >= 3;
        playerTrophies[user.id].finish_over_50k = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] >= 50000));
        playerTrophies[user.id].score_under_minus_30k = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] < -30000));
        
        // Platinum
        playerTrophies[user.id].two_hundred_games = stats.totalHanchans >= 200;
        playerTrophies[user.id].four_top_streak = stats.maxStreak.top >= 4;
        playerTrophies[user.id].twenty_five_no_last = stats.maxStreak.noLast >= 25;
        playerTrophies[user.id].finish_over_70k = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] >= 70000));
        playerTrophies[user.id].avg_rank_2_3 = stats.totalHanchans >= 50 && stats.avgRank <= 2.3;
        let closeGamesCount = 0;
        playerGames.forEach(g => {
            g.scores.forEach(s => {
                const scores = Object.values(s.rawScores).sort((a,b) => b - a);
                if (scores.length > 1 && (scores[0] - scores[1]) < 1000) closeGamesCount++;
            });
        });
        playerTrophies[user.id].ten_close_games = closeGamesCount >= 10;
        playerTrophies[user.id].undefeated_month = Object.keys(monthlyHanchans).some(month => {
            if (monthlyHanchans[month] >= 10) {
                const gamesInMonth = playerGames.filter(g => g.gameDate && g.gameDate.startsWith(month));
                return !gamesInMonth.some(g => g.scores.some(s => {
                    const ranks = {};
                    const scoreGroups = {};
                    Object.entries(s.rawScores).forEach(([pId, score]) => {
                        if (!scoreGroups[score]) scoreGroups[score] = [];
                        scoreGroups[score].push(pId);
                    });
                    const sortedScores = Object.keys(scoreGroups).map(Number).sort((a, b) => b - a);
                    let rankCursor = 0;
                    sortedScores.forEach(score => {
                        const playersInGroup = scoreGroups[score];
                        playersInGroup.forEach(pId => { ranks[pId] = rankCursor; });
                        rankCursor += playersInGroup.length;
                    });
                    return ranks[user.id] === 3;
                }));
            }
            return false;
        });

        // Crystal
        playerTrophies[user.id].four_top_streak_day = Object.keys(dailyPoints).some(date => {
            const gamesOnDate = playerGames.filter(g => g.gameDate && g.gameDate.startsWith(date)).sort((a,b) => (a.createdAt?.seconds || 0) - (b.createdAt?.seconds || 0));
            if (gamesOnDate.length < 1) return false;
            let consecutiveTops = 0;
            for (const game of gamesOnDate) {
                for (const hanchan of game.scores) {
                     const scoreEntries = Object.entries(hanchan.rawScores).sort(([, a], [, b]) => b - a);
                     if(scoreEntries[0][0] === user.id && scoreEntries[0][1] > (scoreEntries[1]?.[1] ?? -Infinity)) {
                         consecutiveTops++;
                     } else {
                         consecutiveTops = 0;
                     }
                     if (consecutiveTops >= 4) return true;
                }
            }
            return false;
        });
        const opponents = users.filter(u => u.id !== user.id);
        const winVsAll = opponents.every(opp => {
            let wins = 0; let losses = 0;
            targetGames.forEach(g => {
                if (g.playerIds.includes(user.id) && g.playerIds.includes(opp.id)) {
                    g.scores.forEach(s => {
                        if (s.rawScores[user.id] > s.rawScores[opp.id]) wins++;
                        if (s.rawScores[user.id] < s.rawScores[opp.id]) losses++;
                    });
                }
            });
            return (wins + losses >= 10) && (wins > losses);
        });
        playerTrophies[user.id].win_vs_all = opponents.length > 0 && winVsAll;
        playerTrophies[user.id].rare_yakuman = playerGames.some(g => g.scores.some(s => s.yakumanEvents && s.yakumanEvents.some(y => y.playerId === user.id && y.yakumans.some(yakuman => ['国士無双十三面待ち', '四暗刻単騎', '純正九蓮宝燈', '大四喜'].includes(yakuman)))));

        // Chaos
        playerTrophies[user.id].four_last_streak_day = Object.keys(dailyPoints).some(date => {
            const gamesOnDate = playerGames.filter(g => g.gameDate && g.gameDate.startsWith(date)).sort((a,b) => (a.createdAt?.seconds || 0) - (b.createdAt?.seconds || 0));
            if (gamesOnDate.length < 1) return false;
             let consecutiveLasts = 0;
             for (const game of gamesOnDate) {
                 for (const hanchan of game.scores) {
                      const scoreEntries = Object.entries(hanchan.rawScores).sort(([, a], [, b]) => b - a);
                      if(scoreEntries.length > 3 && scoreEntries[3][0] === user.id && scoreEntries[3][1] < scoreEntries[2][1]) {
                          consecutiveLasts++;
                      } else {
                          consecutiveLasts = 0;
                      }
                      if (consecutiveLasts >= 4) return true;
                 }
             }
             return false;
        });
        playerTrophies[user.id].minus_200_day = Object.values(dailyPoints).some(p => p <= -200);
        playerTrophies[user.id].busted_minus_50k = playerGames.some(g => g.scores.some(s => s.rawScores[user.id] <= -50000));
        playerTrophies[user.id].yearly_last_player = rankedUsers.length > 0 && rankedUsers[rankedUsers.length - 1].id === user.id;
    });
}

// --- Initial Execution ---
initializeAppAndAuth();
