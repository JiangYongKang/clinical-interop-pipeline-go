package main

// sampleMessages are bundled so the demo runs fully offline. Each entry is one
// realistically-shaped wire message.
var sampleMessages = []sampleMessage{
	{
		Ref:    "hl7-adt-v251",
		Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20260901101500||ADT^A01^ADT_A01|MSG-1001|P|2.5.1
PID|1||MRN-0001||SMITH^JANE||19840312|F
PV1|1|I|WARD-7^201^HOSP-A||||||||||||||||VISIT-7001`,
	},
	{
		Ref:    "hl7-oru-v231",
		Format: "hl7v2",
		Payload: `MSH|^~\&|LAB|CITY|HOSP-A|CITY|20260902093000||ORU^R01|MSG-1002|P|2.3.1
PID|1||MRN-0001||SMITH^JANE||19840312|F
OBR|1||ORD-5001
OBX|1|NM|GLU^Glucose||5.6|mmol/L|||||F|||20260902091500
OBX|2|ST|NOTE^Lab note||fasting sample||||||F|||20260902091500`,
	},
	{
		Ref:    "fhir-r4-new-patient",
		Format: "fhir",
		Payload: `{
  "resourceType": "Bundle",
  "fhirVersion": "R4",
  "source": "HOSP-B",
  "identifier": {"value": "MSG-2001"},
  "meta": {"lastUpdated": "2026-09-03T08:00:00Z"},
  "entry": [
    {"resource": {
      "resourceType": "Patient", "id": "p2",
      "identifier": [{"value": "MRN-0002"}],
      "name": [{"family": "GARCIA", "given": ["CARL"]}],
      "birthDate": "1979-11-30", "gender": "male"
    }},
    {"resource": {
      "resourceType": "Encounter", "id": "e2",
      "identifier": [{"value": "VISIT-8002"}],
      "status": "finished",
      "class": {"code": "EMER"},
      "subject": {"reference": "Patient/p2"},
      "period": {"start": "2026-09-03T07:00:00Z", "end": "2026-09-03T09:30:00Z"}
    }},
    {"resource": {
      "resourceType": "Observation", "id": "o2",
      "identifier": [{"value": "OBS-9002"}],
      "status": "final",
      "code": {"coding": [{"code": "CREA", "display": "Creatinine"}]},
      "subject": {"reference": "Patient/p2"},
      "encounter": {"reference": "Encounter/e2"},
      "effectiveDateTime": "2026-09-03T07:20:00Z",
      "valueQuantity": {"value": 88.4, "unit": "umol/L"}
    }}
  ]
}`,
	},
}

// staleMessages arrive "late" but carry older timestamps; they must never
// overwrite the conclusions already stored above.
var staleMessages = []sampleMessage{
	{
		Ref:    "hl7-stale-old-name",
		Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20260801000000||ADT^A01^ADT_A01|MSG-3001|P|2.5.1
PID|1||MRN-0001||OLDSURNAME^JANE||19840312|F
PV1|1|I|WARD-7||||||||||||||||VISIT-7001`,
	},
}

// badMessages exercise the rejection paths; none may leak payload content.
var badMessages = []sampleMessage{
	{Ref: "bad-version", Format: "hl7v2", Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20200101000000||ADT^A01|MSG-9001|P|2.2
PID|1||MRN-0009||DOE^JOHN||19700101|M
PV1|1|I|WARD-X||||||||||||||||VISIT-9009`},
	{Ref: "bad-missing-field", Format: "fhir", Payload: `{
  "resourceType":"Bundle","fhirVersion":"R4","identifier":{"value":"MSG-9002"},
  "entry":[{"resource":{"resourceType":"Patient","id":"p9","gender":"female"}}]}`},
	{Ref: "bad-json", Format: "fhir", Payload: `{not-json`},
	{Ref: "bad-format", Format: "csv", Payload: "a,b,c"},
}

// typeConflictFHIR reuses observation OBS-9002 but sends a string value where
// the stored value was numeric — must be rejected as type conflict.
var typeConflictFHIR = sampleMessage{
	Ref:    "fhir-type-conflict",
	Format: "fhir",
	Payload: `{
  "resourceType":"Bundle","fhirVersion":"R4","source":"HOSP-B",
  "identifier":{"value":"MSG-2002"},"meta":{"lastUpdated":"2026-09-04T08:00:00Z"},
  "entry":[{"resource":{
    "resourceType":"Observation","id":"o2","identifier":[{"value":"OBS-9002"}],
    "status":"amended",
    "code":{"coding":[{"code":"CREA","display":"Creatinine"}]},
    "subject":{"identifier":{"value":"MRN-0002"}},
    "effectiveDateTime":"2026-09-03T07:20:00Z",
    "valueString":"high"}}]
}`,
}
