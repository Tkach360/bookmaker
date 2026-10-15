package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// локальные копии DTO модуль не зависит от кода проекта и проверяет
// именно то, что видит внешний клиент
type createBetRequest struct {
	UserID  int    `json:"user_id"`
	EventID int    `json:"event_id"`
	Amount  string `json:"amount"`
}

type betResponse struct {
	ID      int    `json:"id"`
	EventID int    `json:"event_id"`
	Amount  string `json:"amount"`
	User    string `json:"user"`
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

func postRaw(raw []byte) (int, []byte, error) {
	resp, err := httpClient.Post(baseURL+"/bet", "application/json", bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

func postBet(req createBetRequest) (int, []byte, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return 0, nil, err
	}

	return postRaw(raw)
}

func getBet(id int) (int, []byte, error) {
	resp, err := httpClient.Get(fmt.Sprintf("%s/bet/%d", baseURL, id))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

// в заглушке сервиса Events событие 1 ещё не произошло (ставки принимаются),
// события 2 и 3 уже произошли (ставки не принимаются)

func TestCreateBet_EventOpen(t *testing.T) {
	code, body, err := postBet(createBetRequest{UserID: 1, EventID: 1, Amount: "100.50"})
	if err != nil {
		t.Fatalf("POST /bet: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", code, body)
	}

	var created betResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decoding response %q: %v", body, err)
	}
	if created.ID == 0 || created.EventID != 1 || created.User != "User_1" {
		t.Errorf("created = %+v, want non-zero id, event 1, user User_1", created)
	}
	if amount, err := strconv.ParseFloat(created.Amount, 64); err != nil || amount != 100.5 {
		t.Errorf("amount = %q, want 100.5", created.Amount)
	}

	// ставка действительно сохранена в Bets: читаем её обратно через gateway
	code, body, err = getBet(created.ID)
	if err != nil {
		t.Fatalf("GET /bet/%d: %v", created.ID, err)
	}
	if code != http.StatusOK {
		t.Fatalf("GET /bet/%d: status = %d, want 200; body: %s", created.ID, code, body)
	}

	var got betResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decoding response %q: %v", body, err)
	}
	if got != created {
		t.Errorf("GET /bet/%d = %+v, want %+v", created.ID, got, created)
	}
}

func TestCreateBet_EventClosed(t *testing.T) {
	for _, eventID := range []int{2, 3} {
		t.Run(fmt.Sprintf("event_%d", eventID), func(t *testing.T) {
			code, body, err := postBet(createBetRequest{UserID: 1, EventID: eventID, Amount: "50"})
			if err != nil {
				t.Fatalf("POST /bet: %v", err)
			}
			if code != http.StatusConflict {
				t.Errorf("status = %d, want 409; body: %s", code, body)
			}
		})
	}
}

func TestCreateBet_EventNotFound(t *testing.T) {
	code, body, err := postBet(createBetRequest{UserID: 1, EventID: 99, Amount: "10"})
	if err != nil {
		t.Fatalf("POST /bet: %v", err)
	}

	// конкретный код не фиксируем (сейчас это ошибка шлюза, лучше было бы 404/422),
	// важно, что ставка не создана и причина понятна из ответа
	if code == http.StatusOK {
		t.Fatalf("status = 200 for unknown event, want an error; body: %s", body)
	}
	if !strings.Contains(string(body), "not found") {
		t.Errorf("body = %s, want it to mention that the event was not found", body)
	}
}

func TestCreateBet_InvalidRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"некорректная сумма", `{"user_id":1,"event_id":1,"amount":"abc"}`},
		{"битый JSON", `{`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body, err := postRaw([]byte(tt.body))
			if err != nil {
				t.Fatalf("POST /bet: %v", err)
			}
			if code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", code, body)
			}
		})
	}
}

// параллельные запросы через весь стек не должны получать одинаковые id
func TestCreateBet_ConcurrentIDsUnique(t *testing.T) {
	const n = 10

	var (
		mu  sync.Mutex
		ids = make(map[int]struct{}, n)
		wg  sync.WaitGroup
	)

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()

			code, body, err := postBet(createBetRequest{UserID: i + 1, EventID: 1, Amount: "10"})
			if err != nil {
				t.Errorf("POST /bet: %v", err)
				return
			}
			if code != http.StatusOK {
				t.Errorf("status = %d, want 200; body: %s", code, body)
				return
			}

			var created betResponse
			if err := json.Unmarshal(body, &created); err != nil {
				t.Errorf("decoding response %q: %v", body, err)
				return
			}

			mu.Lock()
			ids[created.ID] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(ids) != n {
		t.Errorf("got %d unique bet ids, want %d", len(ids), n)
	}
}
