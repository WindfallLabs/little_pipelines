---
version: "0.1.1"
level: pair
components:
  tests/*
processes:
  design: assist
  implementation: assist
  testing: pair
  documentation: assist
  review: assist
  deployment: hint

---

## Notes

- This format is based on [AI-DECLARATION.md](https://ai-declaration.md/en/0.1.1).
- Our use of AI follows our policy (see AI_POLICY.md).
- Claude Sonnet and Microsoft Copilot were used to refactor human-generated code and to generate tests 
- Copilot was also used to detect and squash bugs in many other files; all proposed code has been reviewed manually
