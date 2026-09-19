package app

import (
	"embed"
	"io/fs"
)

//go:embed all:migrations
var migrationsFS embed.FS

// Migrations devolve o sistema de arquivos embutido de migrations, para que
// ferramentas como cmd/migrate possam aplicar/reverter sem duplicar o embed.
func Migrations() (fs.FS, error) {
	return fs.Sub(migrationsFS, "migrations")
}
