OFF_LIMITS_KEYWORDS = [
    "salary", "ctc", "compensation", "pay", "package", "expected salary",
    "notice period", "joining date", "availability", "when can you join",
    "interviewing with", "interview status", "which companies", "offer",
    "why did you leave", "reason for leaving", "opinion on", "thoughts on",
    "what do you think of", "future plans", "would praveen accept",
]

OFF_LIMITS_RESPONSE = (
    "That's not something I can speak to — but you're welcome to reach out "
    "to Praveen directly at pravbala30@gmail.com or connect on LinkedIn at "
    "https://linkedin.com/in/praveen-balasubramanian-69234577"
)

LOW_CONFIDENCE_THRESHOLD = 0.35


def is_off_limits(question):
    question_lower = question.lower()
    return any(keyword in question_lower for keyword in OFF_LIMITS_KEYWORDS)


def is_low_confidence(chunks):
    if not chunks:
        return True
    return all(chunk["score"] < LOW_CONFIDENCE_THRESHOLD for chunk in chunks)
