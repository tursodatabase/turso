random_range() {
    python3 -c "from antithesis.random import get_random; print($1 + get_random() % ($2 - $1 + 1))"
}
