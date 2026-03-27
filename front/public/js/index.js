const STORAGE_KEY = 'propchat_chats_v1';

    let chats = []; // 전체 대화 목록을 저장하는 배열
    let currentSessionId = null; // 현재 활성화된 대화의 세션 ID를 저장하는 변수
    let isComposing = false; // 한글 입력 중인지 여부를 나타내는 플래그
    let isSending = false; // 메시지 전송 중인지 여부를 나타내는 플래그
    let pendingSessionId = null; // 현재 API 요청과 연결된 세션 ID를 저장하는 변수

    const messagesEl = document.getElementById('messages');
    const chatListEl = document.getElementById('chatList');
    const userInputEl = document.getElementById('userInput');
    const sendBtnEl = document.getElementById('sendBtn');
    const newChatBtnEl = document.getElementById('newChatBtn');
    const chipEls = document.querySelectorAll('.chip');

    function generateSessionId() {
      return 'session_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    }

    function getNowTime() {
      return new Date().toLocaleTimeString('ko-KR', {
        hour: '2-digit',
        minute: '2-digit'
      });
    }

    function autoResize(el) {
      el.style.height = 'auto';
      el.style.height = Math.min(el.scrollHeight, 120) + 'px';
    }

    function saveChats() {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(chats));
    }

    function loadChats() {
      const saved = localStorage.getItem(STORAGE_KEY);
      chats = saved ? JSON.parse(saved) : [];
    }

    function getChatBySessionId(sessionId) {
      return chats.find(chat => chat.sessionId === sessionId);
    }

    function createWelcomeMessage(text = '새 대화를 시작합니다! 어떤 부동산 정보가 필요하신가요? 🏠') {
      return {
        role: 'bot',
        text,
        time: getNowTime()
      };
    }

    function createInitialChat() {
      const sessionId = generateSessionId();
      return {
        sessionId,
        title: '새 대화',
        createdAt: Date.now(),
        updatedAt: Date.now(),
        messages: [
          createWelcomeMessage()
        ]
      };
    }

    function escapeHtml(str) {
      return str
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function formatBotText(text) {
        return marked.parse(
            text.trim()
                .replace(/\n{2,}/g, '\n\n') // 문단 줄바꿈 유지
                .replace(/\n/g, ' ') // 문장 줄바꿈 제거
        );
    }

    function addMessageToChat(sessionId, role, text) {
      const chat = getChatBySessionId(sessionId);
      if (!chat) return;

      chat.messages.push({
        role,
        text,
        time: getNowTime()
      });

      chat.updatedAt = Date.now();
      saveChats();
    }

    function updateChatTitle(sessionId, userText) {
      const chat = getChatBySessionId(sessionId);
      if (!chat) return;

      if (chat.title === '새 대화') {
        const cleanTitle = userText.trim().replace(/\s+/g, ' ');
        chat.title = cleanTitle.length > 18 ? cleanTitle.slice(0, 18) + '...' : cleanTitle;
        saveChats();
      }
    }

    function renderSidebar() {
        chatListEl.innerHTML = '';
      
        const sortedChats = [...chats].sort((a, b) => b.updatedAt - a.updatedAt);
      
        sortedChats.forEach(chat => {
          const item = document.createElement('div');
          item.className = 'chat-item' + (chat.sessionId === currentSessionId ? ' active' : '');
          item.innerHTML = `
            <div class="chat-item-content">
              <svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
              <span class="chat-item-text">${escapeHtml(chat.title)}</span>
            </div>
            <div class="chat-item-actions">
              <button class="chat-action-btn edit-btn" title="이름 수정">
                <svg viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="m18.5 2.5 3 3L10 17l-4 1 1-4 11.5-11.5z"/></svg>
              </button>
              <button class="chat-action-btn delete-btn" title="대화 삭제">
                <svg viewBox="0 0 24 24"><polyline points="3,6 5,6 21,6"/><path d="m19,6v14a2,2 0 0,1 -2,2H7a2,2 0 0,1 -2,-2V6m3,0V4a2,2 0 0,1 2,-2h4a2,2 0 0,1 2,2v2"/></svg>
              </button>
            </div>
          `;
      
          // 아이템 전체 클릭
          item.addEventListener('click', (e) => {
            if (e.target.closest('.edit-btn') || e.target.closest('.delete-btn')) {
              return;
            }
      
            if (chat.sessionId === currentSessionId) {
              return;
            }
      
            currentSessionId = chat.sessionId;
            renderSidebar();
            renderMessages(currentSessionId);
          });
      
          const editBtn = item.querySelector('.edit-btn');
          editBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            startEditingChatTitle(item, chat);
          });
      
          const deleteBtn = item.querySelector('.delete-btn');
          deleteBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            deleteChatWithConfirm(chat.sessionId);
          });
      
          chatListEl.appendChild(item);
        });
      }

    function startEditingChatTitle(itemEl, chat) {
      const textEl = itemEl.querySelector('.chat-item-text');
      const actionsEl = itemEl.querySelector('.chat-item-actions');
      
      // 현재 제목을 입력 필드로 변경
      const originalText = chat.title;
      textEl.innerHTML = `<input type="text" class="chat-item-input" value="${escapeHtml(originalText)}" maxlength="30">`;
      
      const inputEl = textEl.querySelector('.chat-item-input');
      inputEl.focus();
      inputEl.select();

      // 액션 버튼 숨기기
      actionsEl.style.display = 'none';

      function finishEditing() {
        const newTitle = inputEl.value.trim();
        if (newTitle && newTitle !== originalText) {
          chat.title = newTitle;
          saveChats();
        }
        
        // 원래 상태로 복원
        textEl.textContent = chat.title;
        actionsEl.style.display = 'flex';
      }

      function cancelEditing() {
        textEl.textContent = originalText;
        actionsEl.style.display = 'flex';
      }

      inputEl.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          finishEditing();
        } else if (e.key === 'Escape') {
          e.preventDefault();
          cancelEditing();
        }
      });

      inputEl.addEventListener('blur', finishEditing);
    }

    function deleteChatWithConfirm(sessionId) {
      const chat = getChatBySessionId(sessionId);
      if (!chat) return;

      if (confirm(`"${chat.title}" 대화를 삭제하시겠습니까?`)) {
        deleteChat(sessionId);
      }
    }

    function deleteChat(sessionId) {
      const chatIndex = chats.findIndex(chat => chat.sessionId === sessionId);
      if (chatIndex === -1) return;

      chats.splice(chatIndex, 1);
      saveChats();

      // 삭제된 대화가 현재 선택된 대화인 경우
      if (currentSessionId === sessionId) {
        if (chats.length > 0) {
          // 가장 최근 대화로 이동
          const sortedChats = [...chats].sort((a, b) => b.updatedAt - a.updatedAt);
          currentSessionId = sortedChats[0].sessionId;
          renderMessages(currentSessionId);
        } else {
          // 대화가 없으면 새 대화 생성
          createNewChat();
        }
      }

      renderSidebar();
    }

    function appendMessageToDOM(role, text, time) {
      const row = document.createElement('div');
      row.className = `msg-row ${role}`;

      if (role === 'bot') {
        row.innerHTML = `
          <div class="msg-avatar">
            <svg viewBox="0 0 24 24"><path d="M3 9.5L12 3l9 6.5V21H15v-5h-6v5H3V9.5z"/></svg>
          </div>
          <div class="msg-content">
            <span class="msg-sender">부동산 AI</span>
            <div class="bubble">${formatBotText(text)}</div>
            <span class="msg-time">${time}</span>
          </div>
        `;
      } else {
        row.innerHTML = `
          <div class="msg-content">
            <span class="msg-sender">나</span>
            <div class="bubble"></div>
            <span class="msg-time">${time}</span>
          </div>
        `;
        row.querySelector('.bubble').textContent = text;
      }

      messagesEl.appendChild(row);
    }

    function renderMessages(sessionId) {
        const chat = getChatBySessionId(sessionId);
        if (!chat) return;
      
        messagesEl.innerHTML = `<div class="date-divider">오늘</div>`;
      
        chat.messages.forEach(message => {
          appendMessageToDOM(message.role, message.text, message.time);
        });
      
        // 추가된 부분: 현재 화면의 세션이 응답을 대기 중이라면 타이핑 인디케이터 다시 표시
        if (sessionId === pendingSessionId) {
          showTypingWithText('답변 생성 중입니다...');
        }
      
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function showTypingWithText(text) {
      const row = document.createElement('div');
      row.className = 'msg-row bot';
      row.id = 'typingRow';
      row.innerHTML = `
        <div class="msg-avatar">
          <svg viewBox="0 0 24 24"><path d="M3 9.5L12 3l9 6.5V21H15v-5h-6v5H3V9.5z"/></svg>
        </div>
        <div class="msg-content">
          <span class="msg-sender">부동산 AI</span>
          <div class="bubble"></div>
        </div>
      `;

      row.querySelector('.bubble').textContent = text;
      messagesEl.appendChild(row);
      messagesEl.scrollTop = messagesEl.scrollHeight;
      return row;
    }

    function removeTyping() {
      const el = document.getElementById('typingRow');
      if (el) el.remove();
    }

    function createNewChat() {
      const newChat = createInitialChat();
      chats.unshift(newChat);
      currentSessionId = newChat.sessionId;

      saveChats();
      renderSidebar();
      renderMessages(currentSessionId);
    }

    async function sendMessage() {
        const text = userInputEl.value.trim();
        if (!text || !currentSessionId || isSending) return;
      
        const requestSessionId = currentSessionId;
        pendingSessionId = requestSessionId; // 추가: 현재 응답을 대기 중인 세션 ID 저장
      
        isSending = true;
        sendBtnEl.disabled = true;
        sendBtnEl.style.opacity = '0.5';
        sendBtnEl.style.cursor = 'not-allowed';
      
        addMessageToChat(requestSessionId, 'user', text);
        updateChatTitle(requestSessionId, text);
      
        renderSidebar();
        renderMessages(requestSessionId); // 여기서 showTypingWithText가 자동으로 호출됩니다.
      
        userInputEl.value = '';
        userInputEl.style.height = 'auto';
      
        const loadingTextBase = '답변 생성 중입니다';
        let dotCount = 1;
      
        // 기존 loadingMsg 참조 대신, 매번 DOM에서 현재 요소를 찾도록 변경
        const loadingInterval = setInterval(() => {
          dotCount = (dotCount % 3) + 1;
          const bubble = document.querySelector('#typingRow .bubble');
          if (bubble) {
            bubble.textContent = loadingTextBase + '.'.repeat(dotCount);
          }
        }, 500);
      
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 60000);
      
          const response = await fetch('http://172.30.1.17:8000/v1/chat', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              prompt: {
                user: text,
                session_id: requestSessionId
              }
            }),
            signal: controller.signal 
          });
      
          clearTimeout(timeoutId);
      
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
      
          const data = await response.json();
      
          if (data.error) {
            throw new Error(data.error);
          }
      
          clearInterval(loadingInterval);
          pendingSessionId = null; // ✨ 추가: 대기 상태 해제
          
          if (currentSessionId === requestSessionId) {
            removeTyping();
          }
      
          const botReply = data.answer || data.response || data.message || '응답을 받지 못했습니다.';
          addMessageToChat(requestSessionId, 'bot', botReply);
      
          if (currentSessionId === requestSessionId) {
            renderSidebar();
            renderMessages(requestSessionId);
          } else {
            renderSidebar();
          }
      
        } catch (error) {
          console.error('API 요청 오류:', error);
      
          clearInterval(loadingInterval);
          pendingSessionId = null; // ✨ 추가: 대기 상태 해제
          
          if (currentSessionId === requestSessionId) {
            removeTyping();
          }
      
          addMessageToChat(requestSessionId, 'bot', '죄송합니다. 현재 서비스에 문제가 있습니다. 잠시 후 다시 시도해주세요.');
          
          if (currentSessionId === requestSessionId) {
            renderSidebar();
            renderMessages(requestSessionId);
          } else {
            renderSidebar();
          }
        } finally {
          isSending = false;
          if (currentSessionId === requestSessionId) {
            sendBtnEl.disabled = false;
            sendBtnEl.style.opacity = '';
            sendBtnEl.style.cursor = '';
          }
        }
    }

    function handleKeydown(e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        if (isComposing || e.isComposing || e.keyCode === 229) {
          return;
        }

        // 전송 중일 때 Enter 키 무시
        if (isSending) {
          e.preventDefault();
          return;
        }

        e.preventDefault();
        sendMessage();
      }
    }

    function initEvents() { //
      userInputEl.addEventListener('input', function () { // 입력할 때마다 텍스트 영역 크기 자동 조절
        autoResize(this);
      });

      userInputEl.addEventListener('keydown', handleKeydown); // Enter 키 이벤트 처리

      userInputEl.addEventListener('compositionstart', () => { // 한글 입력 시작 시 isComposing 플래그 설정
        isComposing = true;
      });

      userInputEl.addEventListener('compositionend', () => { // 한글 입력 종료 시 isComposing 플래그 해제
        isComposing = false;
      });

      sendBtnEl.addEventListener('click', sendMessage); // 전송 버튼 클릭 이벤트
      newChatBtnEl.addEventListener('click', createNewChat); // 새 대화 버튼 클릭 이벤트

      chipEls.forEach(chip => { // 퀵 챗 버튼 클릭 이벤트
        chip.addEventListener('click', () => { // 클릭한 챗 텍스트를 입력 필드에 넣고 전송
          userInputEl.value = chip.textContent.trim();
          autoResize(userInputEl);
          sendMessage();
        });
      });
    }

    function initApp() { // 앱 초기화 - 로컬 스토리지에서 대화 불러오기 및 첫 화면 설정
      loadChats(); // 로컬 스토리지에서 대화 불러오기

      if (!Array.isArray(chats) || chats.length === 0) { // 대화가 없으면 새 대화 생성
        chats = [];
        createNewChat();
      } else { // 가장 최근 대화 선택
        currentSessionId = chats.sort((a, b) => b.updatedAt - a.updatedAt)[0].sessionId;
        renderSidebar();
        renderMessages(currentSessionId);
      }
    }

    initEvents();
    initApp(); 