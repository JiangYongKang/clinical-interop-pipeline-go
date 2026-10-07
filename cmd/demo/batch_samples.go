package main

// goodBatch is one all-or-nothing submission spanning two patients and both
// wire formats. The per-patient business timeline is strictly ascending.
var goodBatch = []sampleMessage{
	{
		Ref: "batch-hl7-adt", Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20260910101500||ADT^A01^ADT_A01|BATCH-001|P|2.5.1
PID|1||MRN-B001||BATCHNAME1^JANE||19840312|F
PV1|1|I|WARD-7||||||||||||||||VISIT-B001`,
	},
	{
		Ref: "batch-hl7-oru", Format: "hl7v2",
		Payload: `MSH|^~\&|LAB|CITY|HOSP-A|CITY|20260911093000||ORU^R01|BATCH-002|P|2.3.1
PID|1||MRN-B001||BATCHNAME1^JANE||19840312|F
OBR|1||ORD-B1
OBX|1|NM|K^Potassium||4.1|mmol/L|||||F|||20260911091500`,
	},
	{
		Ref:    "batch-fhir-patient",
		Format: "fhir",
		Payload: `{
  "resourceType":"Bundle","fhirVersion":"R4","source":"HOSP-C",
  "identifier":{"value":"BATCH-003"},"meta":{"lastUpdated":"2026-09-12T08:00:00Z"},
  "entry":[{"resource":{
    "resourceType":"Patient","identifier":[{"value":"MRN-B002"}],
    "name":[{"family":"BATCHNAME2","given":["MO"]}],
    "birthDate":"1990-05-05","gender":"male"}}]}`,
	},
}

// badBatch is identical to goodBatch except one item carries an unsupported
// HL7 version; the whole batch must be rejected with zero data landing.
var badBatch = []sampleMessage{
	goodBatch[0], goodBatch[1],
	{
		Ref: "batch-bad-version", Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20200101000000||ADT^A01|BATCH-BAD|P|2.2
PID|1||MRN-B002||BATCHSECRET^MO||19900505|M
PV1|1|I|WARD-X||||||||||||||||VISIT-B002`,
	},
}

// historyMessages demonstrate bitemporal reconstruction: V1 then a later
// correction; querying the earlier point must keep returning V1 forever.
var historyMessages = []sampleMessage{
	{
		Ref: "hist-v1", Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20260915080000||ADT^A01^ADT_A01|HIST-V1|P|2.5.1
PID|1||MRN-H001||NAME-V1^J||19840312|F
PV1|1|I|WARD-1||||||||||||||||VISIT-H1`,
	},
	{
		Ref: "hist-v2-correction", Format: "hl7v2",
		Payload: `MSH|^~\&|HOSP-A|CITY|LAB|CITY|20260916080000||ADT^A01^ADT_A01|HIST-V2|P|2.5.1
PID|1||MRN-H001||NAME-V2-CORRECTED^J||19840312|F
PV1|1|I|WARD-1||||||||||||||||VISIT-H1`,
	},
}
