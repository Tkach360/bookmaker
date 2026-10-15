package e2e

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// compose-файл основного проекта: тест поднимает тот же стек, что и docker compose up
	composeFile = "../../docker-compose.yml"
	projectName = "bookmaker-e2e"
)

// baseURL - адрес gateway на хосте, заполняется в TestMain
var baseURL string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("e2e: пропущено в режиме -short")
		os.Exit(0)
	}

	os.Exit(run(m))
}

// вся логика здесь, чтобы defer'ы выполнялись (os.Exit в TestMain их пропускает)
func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Модуль compose, судя по опциям его Up (Recreate, Wait, RunServices),
	// образы из секций build не собирает, поэтому собираем их заранее под тем же
	// именем проекта. Если модуль всё же умеет собирать, эта сборка не помешает:
	// образы возьмутся из кэша.
	if err := dockerComposeBuild(ctx); err != nil {
		log.Printf("e2e: %v", err)
		return 1
	}

	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(projectName),
		compose.WithStackFiles(composeFile),
	)
	if err != nil {
		log.Printf("e2e: creating compose stack: %v", err)
		return 1
	}

	defer func() {
		// отдельный контекст: ctx к этому моменту может быть уже отменён
		if err := stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true)); err != nil {
			log.Printf("e2e: compose down: %v", err)
		}
	}()

	// после остановки стека логи пропадут, поэтому печатаем их при неудаче
	dumpLogs := func() {
		for _, svc := range []string{"gateway", "bets", "events"} {
			c, err := stack.ServiceContainer(context.Background(), svc)
			if err != nil {
				continue
			}
			r, err := c.Logs(context.Background())
			if err != nil {
				continue
			}
			fmt.Fprintf(os.Stderr, "\n===== logs: %s =====\n", svc)
			_, _ = io.Copy(os.Stderr, r)
			_ = r.Close()
		}
	}

	// ждём не просто запуска контейнеров, а готовности приложений: иначе первый
	// запрос может уйти раньше, чем сервисы объявят очереди
	err = stack.
		WaitForService("events", wait.ForLog("Events service is running")).
		WaitForService("bets", wait.ForLog("Bets service is running")).
		WaitForService("gateway", wait.ForListeningPort("8080/tcp")).
		Up(ctx, compose.Wait(true))
	if err != nil {
		log.Printf("e2e: compose up: %v", err)
		dumpLogs()
		return 1
	}

	gateway, err := stack.ServiceContainer(ctx, "gateway")
	if err != nil {
		log.Printf("e2e: getting gateway container: %v", err)
		return 1
	}
	baseURL, err = gateway.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		log.Printf("e2e: getting gateway endpoint: %v", err)
		return 1
	}

	code := m.Run()
	if code != 0 {
		dumpLogs()
	}

	return code
}

// собирает образы сервисов из compose-файла; имя проекта совпадает с тем,
// под которым стек поднимает модуль compose
func dockerComposeBuild(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "-p", projectName, "build")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose build: %w", err)
	}

	return nil
}
