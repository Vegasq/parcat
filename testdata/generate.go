package main

import (
	"log"
	"os"

	"github.com/segmentio/parquet-go"
)

type User struct {
	ID      int64  `parquet:"id"`
	Name    string `parquet:"name"`
	Age     int32  `parquet:"age"`
	Active  bool   `parquet:"active"`
	Score   float64 `parquet:"score"`
}

func main() {
	users := []User{
		{ID: 1, Name: "alice", Age: 30, Active: true, Score: 95.5},
		{ID: 2, Name: "bob", Age: 25, Active: false, Score: 82.3},
		{ID: 3, Name: "charlie", Age: 35, Active: true, Score: 88.7},
		{ID: 4, Name: "diana", Age: 28, Active: true, Score: 91.2},
		{ID: 5, Name: "eve", Age: 42, Active: false, Score: 76.8},
	}

	file, err := os.Create("simple.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[User](file)
	defer writer.Close()

	if _, err := writer.Write(users); err != nil {
		log.Fatal(err)
	}

	log.Println("Generated simple.parquet with 5 users")
}
