// +build ignore

package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"go/build"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// loadPkgFiles loads the go files that should be compiled from a package.
func loadPkgFiles(pkgpath string) (string, []string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}

	pkg, err := build.Import(pkgpath, wd, 0)
	if err != nil {
		return "", nil, err
	}

	// Combine the files with the directory path to get absolute file names.
	files := make([]string, len(pkg.GoFiles))
	for i, fpath := range pkg.GoFiles {
		files[i] = filepath.Join(pkg.Dir, fpath)
	}

	gopath := os.Getenv("GOPATH")
	pkgdir, err := filepath.Rel(filepath.Join(gopath, "src"), pkg.Dir)
	if err != nil {
		return "", nil, err
	}
	return pkgdir, files, nil
}

// hashInputs takes the file inputs and creates a file hash for them.
// This hash is used to cache file outputs.
func hashInputs(inputs []string) (string, error) {
	h := md5.New()
	for _, fpath := range inputs {
		f, err := os.Open(fpath)
		if err != nil {
			return "", err
		}
		io.Copy(h, f)
		if err := f.Close(); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// compile will compile the file from the inputs and output the result to bin.
func compile(bin, pkgdir string) error {
	cmd := exec.Command("go", "build", "-i", "-o", bin, pkgdir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// run will run the binary or, if it does not exist, will compile it from the inputs.
func run(bin, pkgdir string, args []string) error {
	if _, err := os.Stat(bin); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		// Compile the file.
		if err := compile(bin, pkgdir); err != nil {
			return err
		}
	}

	// The file should exist if we get here so try to execute it and pass all of the arguments.
	cmd := exec.Command(bin, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// realMain is the real main function that returns an error so main can print an appropriate message.
// It prevents cluttering main with the same error handling logic.
func realMain() error {
	if len(os.Args) < 2 {
		return errors.New("gorunpkg must be run with at least one argument")
	}

	pkgdir, inputs, err := loadPkgFiles(os.Args[1])
	if err != nil {
		return fmt.Errorf("unable to load package: %s", err)
	}

	// Hash the inputs so that we can find where the binary should be compiled to.
	hash, err := hashInputs(inputs)
	if err != nil {
		return err
	}

	// Compute the filepath and then run the file. This will automatically compile it if needed.
	binpath := filepath.Join(os.TempDir(), "gopkgrun", fmt.Sprintf("%s-%s", filepath.Base(os.Args[1]), hash))
	return run(binpath, pkgdir, os.Args[2:])
}

func main() {
	if err := realMain(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// The binary that failed should have already printed the error output.
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %s.\n", err)
		os.Exit(1)
	}
}
