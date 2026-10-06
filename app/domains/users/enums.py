from enum import Enum


class UserService(str, Enum):
    JD_AND_RESUMES_EXTRACTION = "jd-and-resumes-extraction"
    RESUME_SCREENING = "resume-screening"
    OUTBOUT_CALLING = "outbout-calling"
