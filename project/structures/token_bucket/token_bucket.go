package token_bucket

import (
	"project/keyvalue/config"
	"time"
)

type TokenBucket struct {
	rate     int       // Tokena po sekundi stize u baket
	capacity int       // Kapacitet tokena
	tokens   int       // Trenutno tokena
	last     time.Time // Poslednji zahtev ili inicijalizacija
	lock     chan struct{}
}

func NewTokenBucket() *TokenBucket {

	c := config.GetConfig()

	return &TokenBucket{
		rate:     c.TokenBucketRate,
		capacity: c.TokenBucketCap,
		tokens:   c.TokenBucketCap, // Inicijalno token bucket je upotpunosti pun
		last:     time.Now(),
		lock:     make(chan struct{}, 1),
	}
}

func (b *TokenBucket) Take() bool {
	b.lock <- struct{}{}
	// Sluzi za resavanje race conditiona
	// Znaci da sprecava istovremene goroutine odnosno threadove
	// Zbog paralelnog pristupa kako se ne bismo oslanjali na brzinu izvrsavanja operacija
	// Sto moze prouzrokovati dodatne bugove koje je tesko debugovati
	defer func() {
		<-b.lock
	}()

	// Proverava koliko je sekundi proslo od poslednjeg pristupa
	// Toliko popunjava bucket tokenima po nase rate-u, ako prekoracuje kapacitet
	// Samo ostaje popunjen maksimalno
	now := time.Now()
	d := now.Sub(b.last).Seconds()
	b.tokens += int(float64(b.rate) * d)
	b.last = now

	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}

	if b.tokens < 1 {

		return false
	}
	b.tokens -= 1
	return true
}
