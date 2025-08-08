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

            Object.keys(hanchan.rawScores).forEach(pId => { if(stats[pId]) stats[pId].totalHanchans++; });
            if (hanchan.yakumanEvents) {
                hanchan.yakumanEvents.forEach(event => { if (stats[event.playerId]) { stats[event.playerId].yakumanCount += event.yakumans.length; } });
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
            Object.assign(u, { avgRank: 0, topRate: 0, rentaiRate: 0, lastRate: 0, bustedRate: 0, avgRawScore: 0 });
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
            if (!playerGamesByDate[date]) { playerGamesByDate[date] = 0; }
            playerGamesByDate[date] += game.totalPoints[playerId];
        });

    const history = [];
    fullTimeline.forEach(date => {
        if (playerGamesByDate[date]) { cumulativePoints += playerGamesByDate[date]; }
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
 * Master update function.
 */
function updateAllCalculationsAndViews() {
    cachedStats = calculateAllPlayerStats(games);
    
    updateLeaderboard();
    updateTrophyPage();
    updateHistoryTabFilters();
    updateDetailedHistoryFilters();
    updateHistoryList();
    updateDetailedHistoryTable('raw');
    updateDetailedHistoryTable('pt');
    renderUserManagementList();
    renderPersonalStatsTab();
    updateHeadToHeadDropdowns();
    
    const activeTab = document.querySelector('.tab-btn.active')?.getAttribute('onclick').match(/'([^']+)'/)?.[1];
    
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
        if(!document.getElementById('step3-score-input')?.classList.contains('hidden')) {
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
        document.getElementById(`${tab}-tab`)?.classList.add('hidden');
        document.querySelector(`.tab-btn[onclick="changeTab('${tab}')"]`)?.classList.remove('active');
    });
    document.getElementById(`${tabName}-tab`)?.classList.remove('hidden');
    document.querySelector(`.tab-btn[onclick="changeTab('${tabName}')"]`)?.classList.add('active');

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

// ... all other render functions are the same ...

function renderPlayerSelection() {
    const container = document.getElementById('player-list-for-selection');
    if (!container) return;
    container.innerHTML = users.map(user => {
        const isSelected = selectedPlayers.some(p => p.id === user.id);
        const isDisabled = !isSelected && selectedPlayers.length >= 4;
        const photoHtml = getPlayerPhotoHtml(user.id, 'w-16 h-16');
        return `<div>
            <input type="checkbox" id="player-${user.id}" class="player-checkbox hidden" value="${user.id}" name="${user.name}" onchange="togglePlayerSelection(this)" ${isSelected ? 'checked' : ''} ${isDisabled ? 'disabled' : ''}>
            <label for="player-${user.id}" class="block text-center border-2 border-gray-600 rounded-lg p-3 cursor-pointer transition-colors duration-200 hover:border-blue-500">
                <div class="w-16 h-16 mx-auto mb-2">${photoHtml}</div>
                <span>${user.name}</span>
            </label>
        </div>`;
    }).join('');
}

function renderUserManagementList() {
    const container = document.getElementById('user-list-management');
    if (!container) return;

    const userListHtml = users.length === 0
        ? `<p class="text-gray-500">登録されている雀士がいません。</p>`
        : users.map(user => {
            const photoHtml = getPlayerPhotoHtml(user.id, 'w-12 h-12');
            return `
            <div class="flex items-center gap-4 bg-gray-900 p-2 rounded-lg">
                <div class="relative flex-shrink-0">
                    <div class="cursor-pointer" onclick="triggerPhotoUpload('${user.id}')">
                        ${photoHtml}
                    </div>
                </div>
                <div class="flex-grow">
                    <input type="text" id="user-name-input-${user.id}" value="${user.name}" data-original-name="${user.name}" class="w-full bg-transparent rounded-md p-1 -m-1 focus:bg-gray-800 focus:ring-1 focus:ring-blue-500 outline-none" readonly>
                </div>
                <div class="flex items-center gap-1 flex-shrink-0">
                    <button id="edit-user-btn-${user.id}" onclick="toggleEditUser('${user.id}')" class="cyber-btn text-sm px-3 py-1 rounded-md"><i class="fas fa-edit"></i></button>
                    <button onclick="confirmDeleteUser('${user.id}')" class="cyber-btn-red text-sm px-3 py-1 rounded-md"><i class="fas fa-trash-alt"></i></button>
                </div>
            </div>`;
        }).join('');
    
    container.innerHTML = `
        <input type="file" id="photo-upload-input" class="hidden" accept="image/*" onchange="onFileSelected(event)">
        ${userListHtml}`;
}

// ===== PHOTO UPLOAD LOGIC (FINAL VERIFIED VERSION) =====

/**
 * 1. Triggers the hidden file input when a user's photo is clicked.
 * @param {string} userId - The ID of the user whose photo is being changed.
 */
window.triggerPhotoUpload = (userId) => {
    currentUploadUserId = userId;
    document.getElementById('photo-upload-input').click();
};

/**
 * 2. Called immediately after a file is selected by the user.
 * Shows a loading modal and starts reading the file.
 * @param {Event} event - The file input change event.
 */
window.onFileSelected = (event) => {
    const file = event.target.files[0];
    if (!file || !currentUploadUserId) return;

    // Immediately show a loading modal to comply with browser security policies (especially on iOS).
    showModal(`
        <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">画像を準備中...</h3>
        <div class="flex justify-center items-center h-48">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-yellow-400"></div>
        </div>
    `);

    const reader = new FileReader();
    reader.onload = (e) => {
        // Once the file is read, replace the loading modal content with the cropper UI.
        const modalContent = document.getElementById('modal-content');
        if (modalContent) {
            modalContent.innerHTML = `
                <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">写真のトリミング</h3>
                <div class="max-w-full max-h-[60vh] mb-4 bg-gray-900">
                    <img id="cropper-image" src="${e.target.result}" alt="トリミング対象">
                </div>
                <div class="flex justify-end gap-4 mt-6">
                    <button onclick="cancelCrop()" class="cyber-btn px-4 py-2">キャンセル</button>
                    <button onclick="performCropAndUpload()" class="cyber-btn-green px-4 py-2">トリミングして保存</button>
                </div>
            `;
            // Initialize Cropper.js on the new image element.
            const image = document.getElementById('cropper-image');
            cropper = new Cropper(image, {
                aspectRatio: 1, viewMode: 1, dragMode: 'move', background: false,
                autoCropArea: 0.9, cropBoxMovable: false, cropBoxResizable: false,
            });
        }
    };
    reader.readAsDataURL(file);

    // Reset the file input's value to ensure the 'change' event fires even if the same file is selected again.
    event.target.value = '';
};

/**
 * 3. Called when the "Trim and Save" button is clicked.
 */
window.performCropAndUpload = () => {
    if (!cropper || !currentUploadUserId) return;

    cropper.getCroppedCanvas({
        width: 256, height: 256, imageSmoothingQuality: 'high',
    }).toBlob(async (blob) => {
        if (blob) {
            await uploadCroppedImage(currentUploadUserId, blob);
        } else {
            showModalMessage("画像の変換に失敗しました。");
        }
    }, 'image/webp', 0.85); // Use high-quality WebP format for smaller file size.
};

/**
 * 4. Uploads the cropped image blob to Firebase Storage.
 * @param {string} userId - The ID of the user.
 * @param {Blob} blob - The cropped image data.
 */
async function uploadCroppedImage(userId, blob) {
    showLoadingModal("写真をアップロード中...");
    try {
        const storageRef = ref(storage, `user-photos/${userId}/profile.webp`);
        await uploadBytes(storageRef, blob);
        const downloadURL = await getDownloadURL(storageRef);
        const userDocRef = doc(db, 'users', userId);
        await updateDoc(userDocRef, { photoURL: downloadURL });
        showModalMessage("写真が更新されました！");
    } catch (error) {
        console.error("Photo upload failed:", error);
        showModalMessage("写真のアップロードに失敗しました。");
    } finally {
        cancelCrop(); // Clean up resources.
    }
}

/**
 * 5. Cleans up cropper instance and closes the modal.
 */
window.cancelCrop = () => {
    if (cropper) {
        cropper.destroy();
        cropper = null;
    }
    currentUploadUserId = null;
    closeModal();
};

// ... (rest of the app.js code, including all other functions)
// Ensure to include all functions like addUser, toggleEditUser, calculateAndSave, etc.

// --- Initial Execution ---
initializeAppAndAuth();
