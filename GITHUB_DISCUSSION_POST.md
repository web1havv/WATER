# Post this to: https://github.com/healthyinc/WATER/discussions
# Title: "Introduction and initial technical exploration of WATER"
# Category: General
# Tag: @david (david -at- equilibriacorp.com), @albin (albin -at- cptolabs.com), @vladan (krunic.vladan -at- gmail.com)
# Also tag @pradeeban since he responds to WATER discussions

---

Hi mentors,

I'm Vaibhav Sharma, a [year] CS student from [university]. I came across the WATER project idea and spent the last few days reading through the Niffler and CONTROL-CORE codebases to understand what WATER would need to do.

**What I explored so far:**

I cloned both Niffler and concore and ran them locally. A few things I noticed:

1. Niffler's `modules/workflows/workflow.py` runs all pipeline steps sequentially using `os.chdir()` between modules, which the README itself notes is "currently causing issues by messing with the flow of other modules." This is the core problem WATER needs to fix.

2. I ran the concore demo (`posix_local` mode with `sample.graphml`) and saw how the file-based communication works — controller writes to `./out1/u`, pm reads from `./in1/u` via symlinks. The `mkconcore.py` script auto-generates Dockerfiles and run/build/stop scripts from the GraphML. This shows the orchestration pattern WATER can build on.

3. For the distributed case: when cold-extraction runs on machine A and png-extraction on machine B, there's no mechanism in Niffler to transfer the `/out/cold_extraction/` directory between them. WATER's transfer layer (SSH/rsync or NFS) is what fills this gap.

**Initial architecture thoughts:**

My approach would be to keep WATER as a thin orchestration layer rather than reinventing the communication protocol. Specifically:

- YAML-based workflow definitions (declarative, version-controlled, readable)
- SQLite node registry (lightweight, no external DB dependency for edge deployments)
- Scheduler that's GPU-aware and label-based (so `role=pacs-gateway` steps stay close to the PACS)
- rsync over SSH for inter-node data movement (efficient for large DICOM archives)
- FastAPI REST control plane for node registration and workflow submission
- A `graphml_to_water` converter so existing concore studies can be imported directly

I've also written a small proof-of-concept that validates this architecture (schema + registry + scheduler + basic execution). Happy to share the repo link if that would be useful.

**One question for the mentors:**

Reading the WATER README — "The focus is on building a utility framework and an abstraction that facilitates the edge workflow execution rather than a simple decentralized data storage" — I want to make sure I'm reading the priority correctly: the goal is workflow routing/scheduling, not replacing the underlying DICOM storage layer (like a VNA or PACS). Is that right?

Looking forward to your feedback. I'll be posting more specific questions in separate threads as I get further into the implementation.

Thanks,
Vaibhav

---
# IMPORTANT NOTES before posting:
# 1. Fill in [year] and [university]
# 2. This post is SHORT and SPECIFIC — exactly what Pradeeban's policy requires
# 3. The question at the end is genuine (not AI-generated filler)
# 4. DO NOT use AI to generate this post — type it yourself even if imperfect
# 5. Do NOT cc this to anyone's email (per contributor guide)
# 6. After posting, check back every 24-48 hours for responses
# 7. After mentor responds, post the GitHub link to your PoC repo
