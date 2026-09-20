// Command migrate aplica ou reverte as migrations versionadas do banco.
//
// As migrations também são aplicadas automaticamente no start da aplicação;
// este comando permite controlar o lifecycle manualmente (ex.: reverter antes
// de recriar o schema em desenvolvimento).
//
//	Uso: wallet-migrate up|down
//	Ambiente: DATABASE_URL (default: postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/app"
	"desafio-go/internal/storage/postgres"
)

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "up" && os.Args[1] != "down") {
		fmt.Fprintln(os.Stderr, "uso: wallet-migrate up|down")
		os.Exit(2)
	}
	direction := os.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fatal("pool", err)
	}
	defer pool.Close()

	files, err := app.Migrations()
	if err != nil {
		fatal("migrations fs", err)
	}
	mig := postgres.NewMigrator(pool, files)

	switch direction {
	case "up":
		if err := mig.Up(ctx); err != nil {
			fatal("up", err)
		}
		fmt.Println("migrations aplicadas com sucesso")
	case "down":
		v, err := mig.Down(ctx)
		if err != nil {
			fatal("down", err)
		}
		if v == 0 {
			fmt.Println("nada a reverter")
		} else {
			fmt.Printf("migration %04d revertida com sucesso\n", v)
		}
	}
}

func fatal(step string, err error) {
	slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("migrate failed", "step", step, "error", err.Error())
	os.Exit(1)
}
