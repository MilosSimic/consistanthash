package main

import "fmt"

func main() {
	r := New(20, nil)
	r.Add("127.0.0.1", "127.0.0.2", "127.0.0.3")
	fmt.Println(r.Get("KeyA", "KeyB", "KeyABV", "KeyACVBF"))
}
