# The requests scripts/test-examples.sh sends to this example, in order.
json='Content-Type: application/json'
req GET /health
# A stream that takes each note committed from now on, for 4 s; the notes
# are written once it has subscribed.
curl -s -N -m 4 "http://127.0.0.1:$port/events" >"$out/events" &
events=$!
until grep -q subscribed "$out/events" 2>/dev/null; do sleep 0.05; done
req POST /notes -H "$json" -d '{"text": "first note"}'
# The receiver takes 3 s over this one, past the webhook's 1 s budget: the
# note is committed, and the answer says the webhook timed out.
req POST /notes -H "$json" -d '{"text": "a slow note"}'
req POST /notes -H "$json" -d '{"text": "50% off, the_end"}'
req GET /notes/1
req GET /notes/9
req GET '/notes?q=note'
req GET '/notes?q=50%25'
req GET '/notes?q=_'
req POST /notes -H "$json" -d '{"text": ""}'
req POST /notes -H "$json" -d '{"title": "x"}'
big=$(printf '{"text": "%5000s"}' '')
req POST /notes -H "$json" -d "$big"
wait "$events" || true
echo "events: $(tr '\n' '|' <"$out/events")"
