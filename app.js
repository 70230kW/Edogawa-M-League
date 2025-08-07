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

                        if (currentRank <= 1) {
                            stats[pId].currentStreak.rentai++;
                        } else {
                            stats[pId].currentStreak.rentai = 0;
                        }
                        if (currentRank === 0) {
                            stats[pId].currentStreak.top++;
                        } else {
                            stats[pId].currentStreak.top = 0;
                        }
                        if (currentRank === 3) {
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
                <select id="history-${prefix}-year-filter" class="mt-1 block w-full rounded-md" onchange="updateDetailedHistoryTable('${prefix}')"></select>
            </div>
            <div class="flex-1">
                <label for="history-${prefix}-month-filter" class="block text-sm font-medium text-gray-400">月</label>
                <select id="history-${prefix}-month-filter" class="mt-1 block w-full rounded-md" onchange="updateDetailedHistoryTable('${prefix}')"></select>
            </div>
            <div class="flex-1">
                <label for="history-${prefix}-player-filter" class="block text-sm font-medium text-gray-400">雀士</label>
                <select id="history-${prefix}-player-filter" class="mt-1 block w-full rounded-md" onchange="updateDetailedHistoryTable('${prefix}')"></select>
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
            <div id="stat-cards-container" class="grid grid-cols-2 md:grid-cols-3 gap-4 text-center"></div>
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
            </div>
        `}).join('');
    
    container.innerHTML = `
        <input type="file" id="photo-upload-input" class="hidden" accept="image/*" onchange="onFileSelected(event)">
        ${userListHtml}
    `;
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
        if (rankedUsers.length <= 1 || minMax[field].min === minMax[field].max) return '';
        if (statFields[field] === 'higher') {
            if (value === minMax[field].max) return 'text-rank-1';
            if (value === minMax[field].min) return 'text-rank-4';
        } else if (statFields[field] === 'lower') {
            if (value === minMax[field].min) return 'text-rank-1';
            if (value === minMax[field].max) return 'text-rank-4';
        }
        return '';
    };

    if (leaderboardBody) {
        leaderboardBody.innerHTML = rankedUsers.map((user, index) => {
            const photoHtml = getPlayerPhotoHtml(user.id, 'w-8 h-8');
            return `
                <tr class="hover:bg-gray-800 font-m-gothic text-xs md:text-sm">
                    <td class="px-2 py-4 whitespace-nowrap text-right sticky-col-1">${index + 1}</td>
                    <td class="px-2 py-4 whitespace-nowrap text-left font-medium text-blue-400 cursor-pointer hover:underline sticky-col-2" onclick="showPlayerStats('${user.id}')">
                        <div class="flex items-center gap-3">${photoHtml}<span>${user.name}</span></div>
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

    const currentYear = yearSelect.value;
    const currentMonth = monthSelect.value;
    const currentPlayer = playerSelect.value;

    const yearOptions = getGameYears().map(year => `<option value="${year}">${year}年</option>`).join('');
    yearSelect.innerHTML = `<option value="all">すべて</option>${yearOptions}`;
    if (Array.from(yearSelect.options).some(opt => opt.value === currentYear)) {
        yearSelect.value = currentYear;
    }

    if (monthSelect.options.length === 0) {
        const monthOptions = Array.from({length: 12}, (_, i) => i + 1).map(m => `<option value="${m}">${m}月</option>`).join('');
        monthSelect.innerHTML = `<option value="all">すべて</option>${monthOptions}`;
    }
    if (Array.from(monthSelect.options).some(opt => opt.value === currentMonth)) {
        monthSelect.value = currentMonth;
    }

    const playerOptions = users.map(u => `<option value="${u.id}">${u.name}</option>`).join('');
    playerSelect.innerHTML = `<option value="all">すべて</option>${playerOptions}`;
    if (Array.from(playerSelect.options).some(opt => opt.value === currentPlayer)) {
        playerSelect.value = currentPlayer;
    }
}

function updateDetailedHistoryFilters() {
    const prefixes = ['raw', 'pt'];
    const today = new Date();
    const currentYearStr = today.getFullYear().toString();
    const currentMonthStr = (today.getMonth() + 1).toString();

    prefixes.forEach(prefix => {
        const yearSelect = document.getElementById(`history-${prefix}-year-filter`);
        const monthSelect = document.getElementById(`history-${prefix}-month-filter`);
        const playerSelect = document.getElementById(`history-${prefix}-player-filter`);

        if (!yearSelect || !monthSelect || !playerSelect) return;

        const isInitialLoad = !yearSelect.value;

        const yearOptions = getGameYears().map(year => `<option value="${year}">${year}年</option>`).join('');
        const playerOptions = users.map(u => `<option value="${u.id}">${u.name}</option>`).join('');
        
        const preservedYear = yearSelect.value;
        const preservedMonth = monthSelect.value;
        const preservedPlayer = playerSelect.value;

        yearSelect.innerHTML = `<option value="all">すべて</option>${yearOptions}`;
        playerSelect.innerHTML = `<option value="all">すべて</option>${playerOptions}`;

        if (monthSelect.options.length <= 1) {
            const monthOptions = Array.from({length: 12}, (_, i) => i + 1).map(m => `<option value="${m}">${m}月</option>`).join('');
            monthSelect.innerHTML = `<option value="all">すべて</option>${monthOptions}`;
        }

        if (isInitialLoad) {
            yearSelect.value = Array.from(yearSelect.options).some(opt => opt.value === currentYearStr) ? currentYearStr : 'all';
            monthSelect.value = currentMonthStr;
            playerSelect.value = 'all';
        } else {
            yearSelect.value = preservedYear;
            monthSelect.value = preservedMonth;
            playerSelect.value = preservedPlayer;
        }
    });
}

window.updateHistoryList = () => {
    const container = document.getElementById('history-list-container');
    if (!container) return;

    const yearFilter = document.getElementById('history-year-filter').value;
    const monthFilter = document.getElementById('history-month-filter').value;
    const playerFilter = document.getElementById('history-player-filter').value;

    let filteredGames = [...games];

    if (yearFilter !== 'all') {
        filteredGames = filteredGames.filter(game => (game.gameDate || '').substring(0, 4) === yearFilter);
    }
    if (monthFilter !== 'all') {
        filteredGames = filteredGames.filter(game => {
            if (!game.gameDate) return false;
            const parts = game.gameDate.split('/');
            return parts.length > 1 && parts[1] === monthFilter;
        });
    }
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

function updateDetailedHistoryTable(prefix) {
    const listContainer = document.getElementById(`history-${prefix}-list`);
    const yearFilter = document.getElementById(`history-${prefix}-year-filter`).value;
    const monthFilter = document.getElementById(`history-${prefix}-month-filter`).value;
    const playerFilter = document.getElementById(`history-${prefix}-player-filter`).value;

    if (!listContainer) return;

    const allHanchans = [];
    games.forEach(game => {
        if (!game.scores) return;
        game.scores.forEach((hanchan, index) => {
            allHanchans.push({
                date: game.gameDate || new Date(game.createdAt.seconds * 1000).toLocaleString('ja-JP'),
                hanchanNum: index + 1,
                playerIds: game.playerIds,
                rawScores: hanchan.rawScores,
                points: hanchan.points
            });
        });
    });

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

    const dataType = prefix === 'raw' ? 'rawScores' : 'points';
    let tableHtml = `<table class="min-w-full divide-y divide-gray-700 font-m-gothic text-xs md:text-sm">
        <thead class="bg-gray-900 text-xs md:text-sm font-medium text-gray-400 uppercase tracking-wider">
            <tr>
                <th class="px-2 py-3 text-left whitespace-nowrap">日時</th>
                ${users.map(u => `<th class="px-2 py-3 text-right whitespace-nowrap">${u.name}</th>`).join('')}
            </tr>
        </thead>
        <tbody class="divide-y divide-gray-700">`;

    if (filteredHanchans.length === 0) {
        tableHtml += `<tr><td colspan="${users.length + 1}" class="text-center py-4 text-gray-500">該当する履歴がありません</td></tr>`;
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
            if (!scores) return;

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
    listContainer.innerHTML = tableHtml;
}

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
        .map(u => `<div class="mr-4"><input type="checkbox" id="compare-${u.id}" value="${u.id}" class="comparison-checkbox" onchange="updateComparisonCharts('${playerId}')"><label for="compare-${u.id}" class="ml-1">${u.name}</label></div>`)
        .join('');

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
            <div class="cyber-card p-4 sm:p-6"><h3 class="cyber-header text-xl font-bold mb-4 text-center">順位分布</h3><div class="w-full h-64 mx-auto"><canvas id="rank-chart-personal"></canvas></div></div>
            <div class="cyber-card p-4 sm:p-6"><h3 class="cyber-header text-xl font-bold mb-4 text-center">ポイント推移</h3><div class="w-full h-64 mx-auto"><canvas id="point-history-chart-personal"></canvas></div></div>
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
                    playersInGroup.forEach(pId => { ranks[pId] = rankCursor; });
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
                    <th class="px-4 py-2 text-left">日時</th><th class="px-4 py-2 text-right">着順</th><th class="px-4 py-2 text-right">素点</th><th class="px-4 py-2 text-right">ポイント</th>
                </tr></thead>
                <tbody class="divide-y divide-gray-700">`;
    
    playerHanchans.reverse().forEach(h => {
        tableHtml += `<tr>
            <td class="px-4 py-2 whitespace-nowrap">${h.date}</td><td class="px-4 py-2 text-right">${h.rank}</td><td class="px-4 py-2 text-right">${h.rawScore.toLocaleString()}</td><td class="px-4 py-2 text-right ${h.point >= 0 ? 'text-green-400' : 'text-red-400'}">${h.point.toFixed(1)}</td>
        </tr>`;
    });

    tableHtml += `</tbody></table></div>`;
    container.innerHTML = tableHtml;
}

function renderStatsCharts(mainPlayerId, comparisonIds) {
    const colors = ['#58a6ff', '#52c569', '#f5655f', '#f2cc8f', '#e0aaff', '#9bf6ff'];
    
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
            label: cachedStats[mainPlayerId].name, data: mainPlayerData, borderColor: colors[0], backgroundColor: colors[0] + '33', fill: true, tension: 0.1
        });
        comparisonIds.forEach((id, index) => {
            const playerData = getPlayerPointHistory(id, fullTimeline);
            pointHistoryDatasets.push({
                label: cachedStats[id].name, data: playerData, borderColor: colors[(index + 1) % colors.length], backgroundColor: colors[(index + 1) % colors.length] + '33', fill: true, tension: 0.1
            });
        });

        if (personalPointHistoryChart) personalPointHistoryChart.destroy();
        personalPointHistoryChart = new Chart(personalChartCanvas.getContext('2d'), {
            type: 'line', data: { labels: fullTimeline, datasets: pointHistoryDatasets },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#c9d1d9' }}}, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } } } }
        });
    }

    const rankChartCanvas = document.getElementById('rank-chart-personal');
    if(rankChartCanvas) {
        const rankDatasets = [];
        rankDatasets.push({
            label: cachedStats[mainPlayerId].name, data: cachedStats[mainPlayerId].ranks, backgroundColor: colors[0],
        });
        comparisonIds.forEach((id, index) => {
            rankDatasets.push({
                label: cachedStats[id].name, data: cachedStats[id].ranks, backgroundColor: colors[(index + 1) % colors.length],
            });
        });

        if (personalRankChart) personalRankChart.destroy();
        personalRankChart = new Chart(rankChartCanvas.getContext('2d'), {
            type: 'bar', data: { labels: ['1位', '2位', '3位', '4位'], datasets: rankDatasets },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top', labels: { color: '#c9d1d9' }}}, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#30363d' } } } }
        });
    }
}

// ----- ここから新しい写真アップロード処理 -----

let cropper = null;
let currentUploadUserId = null;

/**
 * 1. 雀士の写真がクリックされたときに呼ばれ、ファイル選択ダイアログを開く
 * @param {string} userId - 対象ユーザーのID
 */
window.triggerPhotoUpload = (userId) => {
    currentUploadUserId = userId;
    document.getElementById('photo-upload-input').click();
};

/**
 * 2. ファイルが選択された直後に呼ばれる
 * @param {Event} event - ファイル選択イベント
 */
window.onFileSelected = (event) => {
    const file = event.target.files[0];
    if (!file || !currentUploadUserId) return;

    // iPhoneの仕様に対応するため、まずローディング画面のモーダルを即座に表示
    showModal(`
        <h3 class="cyber-header text-xl font-bold text-yellow-300 mb-4">画像を準備中...</h3>
        <div class="flex justify-center items-center h-48">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-yellow-400"></div>
        </div>
    `);

    // その裏で画像のデータを読み込む
    const reader = new FileReader();
    reader.onload = (e) => {
        // 読み込み完了後、モーダルの内容をトリミング画面に差し替える
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
            // Cropper.jsを初期化
            const image = document.getElementById('cropper-image');
            cropper = new Cropper(image, {
                aspectRatio: 1, viewMode: 1, dragMode: 'move', background: false,
                autoCropArea: 0.9, cropBoxMovable: false, cropBoxResizable: false,
            });
        }
    };
    reader.readAsDataURL(file);

    // 同じファイルを再度選択してもイベントが発火するように値をリセット
    event.target.value = '';
};

/**
 * 3. 「トリミングして保存」ボタンが押されたときの処理
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
    }, 'image/webp', 0.85);
};

/**
 * 4. トリミングされた画像をFirebaseにアップロードする
 * @param {string} userId - 対象ユーザーID
 * @param {Blob} blob - 画像データ
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
        cancelCrop(); // 最後にリソースを解放
    }
}

/**
 * 5. キャンセル処理とリソースの解放
 */
window.cancelCrop = () => {
    if (cropper) {
        cropper.destroy();
        cropper = null;
    }
    currentUploadUserId = null;
    closeModal();
};

// ----- ここまで新しい写真アップロード処理 -----
