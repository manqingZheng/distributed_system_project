package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
    "math/rand"
    "log"
)

func read(path string) map[[10]byte][90]byte {
	file, err := os.Open(path)

	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	defer file.Close()

	m := make(map[[10]byte][90]byte)
    key_buf := make([]byte, 10)
    value_buf := make([]byte, 90)

	for {
		_, err := file.Read(key_buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
				os.Exit(-1)
			}
			break
		}
		_, err = file.Read(value_buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
				os.Exit(-1)
			}
			break
		}

		var key_arr [10]byte
		copy(key_arr[:], key_buf)
		var value_arr [90]byte
		copy(value_arr[:], value_buf)
		m[key_arr] = value_arr
	}
	return m
}

func get_sortarray(m map[[10]byte][90]byte) [][]byte {
    var s [][]byte
	for k := range m {
        k_slice := make([]byte, len(k))
        copy(k_slice, k[:])
		s = append(s, k_slice)
	}
	return s
}

func quicksort(a [][]byte) [][]byte {
	if len(a) < 2 {
		return a
	}

	left, right := 0, len(a) - 1

	pivot := rand.Int() % len(a)

	a[pivot], a[right] = a[right], a[pivot]

	for i := range a {
		if bytes.Compare(a[i], a[right]) < 0 {
			a[left], a[i] = a[i], a[left]
			left++
		}
	}

	a[left], a[right] = a[right], a[left]

	quicksort(a[:left])
	quicksort(a[left+1:])

	return a
}

func write_sorted(s [][]byte, m map[[10]byte][90]byte, output_name string) {
	f, err := os.Create(output_name)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer f.Close()

	for _, key_slice := range s {
		var key_arr [10]byte
		copy(key_arr[:], key_slice)

		_, err := f.Write(key_slice)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		
        value_arr := m[key_arr]
		value_slice := value_arr[:]
		_, err = f.Write(value_slice)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	}
}

func main() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])
	m := read(os.Args[1])
	s := get_sortarray(m)
    s = quicksort(s)
	write_sorted(s, m, os.Args[2])
}
