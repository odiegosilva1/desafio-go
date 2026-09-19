package postgres

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsParsingAndOrdering(t *testing.T) {
	fsys := fstest.MapFS{
		"README.md":              {Data: []byte("ignored")},
		"0001_init.up.sql":       {Data: []byte("up1")},
		"0001_init.down.sql":     {Data: []byte("down1")},
		"0002_wagering.up.sql":   {Data: []byte("up2")},
		"0002_wagering.down.sql": {Data: []byte("down2")},
		"0010_extra.up.sql":      {Data: []byte("up10")},
		"not_a_migration.sql":    {Data: []byte("x")},
	}

	migs, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}

	wantVersions := []int{1, 2, 10}
	gotVersions := make([]int, 0, len(migs))
	for _, m := range migs {
		gotVersions = append(gotVersions, m.version)
	}
	if !reflect.DeepEqual(gotVersions, wantVersions) {
		t.Fatalf("versions = %v, want %v", gotVersions, wantVersions)
	}

	if migs[0].name != "init" || migs[0].up != "up1" || migs[0].down != "down1" {
		t.Fatalf("migration 0001 = %+v", migs[0])
	}
	if migs[1].name != "wagering" || migs[1].up != "up2" {
		t.Fatalf("migration 0002 = %+v", migs[1])
	}
	if migs[2].down != "" {
		t.Fatalf("migration 0010 with only up should have empty down, got %+v", migs[2])
	}
}

func TestLoadMigrationsRejectsMissingUp(t *testing.T) {
	fsys := fstest.MapFS{
		"0003_only.down.sql": {Data: []byte("down3")},
	}
	if _, err := loadMigrations(fsys); err == nil {
		t.Fatal("expected error for migration without .up.sql, got nil")
	}
}

func TestLoadMigrationsEmptyFS(t *testing.T) {
	if _, err := loadMigrations(fstest.MapFS{}); err == nil {
		t.Fatal("expected error for empty fs, got nil")
	}
}

func TestParseMigrationName(t *testing.T) {
	cases := []struct {
		name        string
		wantVersion int
		wantDir     string
		wantName    string
		wantOK      bool
	}{
		{"0001_init.up.sql", 1, migrationUp, "init", true},
		{"0001_init.down.sql", 1, migrationDown, "init", true},
		{"0010_a_b.up.sql", 10, migrationUp, "a_b", true},
		{"bad.sql", 0, "", "", false},
		{"no_underscore.sql", 0, "", "", false},
		{"0001_init.txt", 0, "", "", false},
		{"0015_private.up.down.sql", 0, "", "", false},
	}
	for _, c := range cases {
		v, dir, name, ok := parseMigrationName(c.name)
		if v != c.wantVersion || dir != c.wantDir || name != c.wantName || ok != c.wantOK {
			t.Errorf("%s: got (%d, %q, %q, %v), want (%d, %q, %q, %v)",
				c.name, v, dir, name, ok, c.wantVersion, c.wantDir, c.wantName, c.wantOK)
		}
	}
}

func TestParseMigrationNameBadVersion(t *testing.T) {
	if _, _, _, ok := parseMigrationName("001a_init.up.sql"); ok {
		t.Fatal("expected ok=false for non-numeric version")
	}
}

func TestRepoMigrationsHaveDown(t *testing.T) {
	// sanity: os scripts versionados do repositório têm .down.sql para toda versão
	fsys := os.DirFS("../../app/migrations")
	migs, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations(repo): %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("repo has no migrations")
	}
	for _, m := range migs {
		if strings.TrimSpace(m.down) == "" {
			t.Errorf("migration %04d missing down script", m.version)
		}
	}
}
