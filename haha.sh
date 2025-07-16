it=0
for x in {a..z}
do
	for y in {a..z}
	do
		s=$x$y
		cargo run $1 --experimental-indexes "INSERT INTO t VALUES (replace(zeroblob(20), x'00', 'a') || '$s', $it)"
		((it++))
	done
done
