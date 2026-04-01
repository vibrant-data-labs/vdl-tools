import os
from textwrap import dedent
import pandas as pd
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    PromptResponseCacheSQL,
)
import vdl_tools.shared_tools.project_config as pc

import openai

paths = pc.get_paths()
# %%
openai.api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI()
model = "gpt-4.1-mini"

few_shot_df = pd.read_excel(paths["labeled_data"], engine="openpyxl")
few_shot_df = few_shot_df[~(few_shot_df["examples"].isna())][
    ["examples", "text_for_one_earth"]
].copy()
examples_txt = ""
for _, row in few_shot_df.iterrows():
    examples_txt += f"{row['text_for_one_earth']} \n Output:\n {row['examples']}\n\n"
OLD_PROMPT = dedent(
    f"""Evaluate each organization and assign it to one of the following categories based on its relationship to K–12 
    and community-based education:\n
    Horizon 1 (H1) – Organizations that improve, optimize, or support existing education systems or informal enrichment 
    models without altering the underlying structure. These may include schools, museums, zoos, or programs that deliver 
    traditional or supplementary learning experiences. \n
    
    Horizon 2 (H2) – Organizations that experiment with or scale emerging models that challenge traditional assumptions 
    but still connect to the current system. These might involve blended learning, learner-centered models, 
    interdisciplinary programs, or community-driven education initiatives. \n
    
    Horizon 3 (H3) – Organizations that aim to radically reimagine education—shifting power to learners, rethinking what 
    counts as learning, and creating entirely new systems or paradigms. These may decouple learning from schools, 
    disrupt age-based grouping, or deeply integrate purpose, agency, and equity. \n
    
    Not Relevant– If the organization does not focus on K–12 or community-based education—or is unrelated to education 
    altogether (e.g., not part of the philanthropic, public, or private investments that support innovation within 
    and around education)—classify it as Not Relevant. \n
    Focus only on Pre-K through 12th grade and community education; disregard colleges, universities, or adult learning unless explicitly 
    community college–based. Pay attention to terms such as"college preparatory"—these are still within scope. 
    Include organizations that act as ecosystem enablers or indirect education innovators, even if they do not directly 
    deliver instruction. Their work may be essential to enabling innovation. These may include: \n
    -Organizations working on policy, infrastructure, or systemic support (e.g., data systems, funding models, learning ecosystems).
    -Funders or coalitions that explicitly focus on K–12 or community education innovation. \n
    
    If classification is unclear or doubtful, assign the organization to the lowest plausible Horizon 
    (e.g., H1 if it could be H1 or Not Relevant). Favor inclusion over exclusion. \n
    
    Output a string with the category assigned (H1, H2, H3, or Not Relevant) and nothing else.\n
    Here are some examples to help you understand the categories:\n
    {examples_txt}
    """
).strip()

BASE_PROMPT = dedent(
    f"""
    Classify each organization as Relevant or Not Relevant to the education ecosystem.
    Guidelines:

    1. Focus only on relevance to the education ecosystem—whether the organization contributes to or enables educational 
    activity or innovation (H1, H2, or H3). Do not classify by horizon—just determine if it is Relevant or Not Relevant.\n
    2. Consider the following as Relevant:\n
    ### Museums, public libraries, discovery centers, science centers
    ### College prep enablers, charter schools
    ### Youth development organizations such as Big Brothers Big Sisters, Boys & Girls Clubs
    ### Any PreK–12 or community education organization
    \n
    3. Focus on organizations serving PreK through 12th grade, including teen-focused community or informal learning efforts.\n
    4. Disregard colleges, universities, or adult education providers unless they also offer teen or youth learning programs.\n
    5. Exclude organizations focused solely on corporate training, adult workforce upskilling, or professional certification, 
    unless there is a clear youth-facing or K–12 component.\n
    6. Include ecosystem enablers or indirect education innovators, even if they do not directly deliver instruction. This includes:\n
    ### Policy, infrastructure, or systemic support organizations (e.g., data systems, funding models, learning ecosystems)
    ### Funders, coalitions, or technical assistance providers explicitly focused on K–12 or community education innovation
\n
    7. If an organization’s core activity is entertainment (e.g., gaming, media, events), only consider it Relevant
    if it has a clear educational mission or direct engagement with child or youth learning.\n
    8. Include EdTech platforms or tools that support learning, instruction, or assessment in ways applicable to K–12 or 
    teen learners—even if they are also used in higher education. Tools focused on writing, inquiry, engagement, 
    or classroom support are typically relevant.\n
    9. Include college and career readiness platforms and tools that support K–12 students in planning education and 
    career pathways. These tools are relevant even if they also serve employers or higher education institutions.\n
    8. If the classification is unclear or doubtful, default to Relevant.\n

    Output Format:\n
    Output a string with the category assigned: "Relevant" or "Not Relevant"—nothing else.\n
"""
).strip()

H3_PROMPT = dedent(
    f"""You are a binary relevance classifier for the LearnerStudio Education Ecosystem.

Your task is to determine whether an organization is aligned with Horizon 3 (H3) education — a visionary, future-ready learning paradigm that shifts away from traditional, standardized schooling toward student-driven, competency-based mastery. H3 emphasizes equity, personalized learning, real-world application, learner agency, creativity, and critical thinking, preparing students to thrive in an unpredictable future.

The organizations you will classify span the full education ecosystem. H3 alignment can appear in any sector or org type — it is about the orientation and philosophy of the work, not the category alone.

## Label as 1 (H3-relevant) if the organization meaningfully advances any of the following:

**Learner-centered models and pedagogy**
- Competency-based, mastery-based, or proficiency-based learning where students advance upon demonstrated mastery, not seat time
- Project-based, experiential, or inquiry-driven learning where the project or experience is the primary vehicle for knowledge-building — not a supplemental activity
- Personalized or adaptive learning models that tailor pace, path, or content to individual learners at a systemic level
- Alternative school models: micro-schools, Montessori, Waldorf, Reggio Emilia, democratic schools, multi-age learning environments
- Virtual, hybrid, or location-independent models that expand access and learner flexibility
- Self-directed or interest-driven learning communities

**Tools and infrastructure that enable H3 learning**
- Adaptive or AI-driven instructional platforms that personalize learning in real time
- Embedded formative and mastery assessment tools that replace or supplement standardized grading with continuous evidence of learning
- Digital credentialing, mastery transcripts, micro-credentials, or portfolio systems that document competencies beyond GPA
- Learning and employment records that create portable, learner-owned profiles across institutions
- EdTech platforms centered on inquiry, curiosity, critical thinking, or student agency
- Learning space design that enables flexible, collaborative, or experiential instruction (makerspaces, STEAM labs, flexible classroom environments)

**Educator workforce transformation toward H3**
- Professional development explicitly oriented toward learner-centered, competency-based, or culturally responsive pedagogy — not just general teacher coaching or mentoring
- Innovative staffing models that restructure the educator role (team teaching, learning facilitators, differentiated roles) to enable new pedagogical models
- Alternative teacher pipelines or certification reform that modernizes what it means to be an educator
- Leadership development explicitly oriented toward school redesign, systems transformation, or H3 implementation

**Ecosystem infrastructure explicitly for H3**
- Skills and competency frameworks, Portrait of a Graduate models, or alternative credentialing standards
- Research and development that generates evidence for learner-centered or equity-focused innovations
- Prototyping, incubation, or acceleration of new school models or H3-aligned EdTech
- Field catalysts and innovation intermediaries that scale proven learner-centered innovations
- Outcomes-based finance or pay-for-success models tied to learner-centered outcomes
- Policy and advocacy work explicitly aimed at enabling H3 practices (competency-based policy, alternative credentialing, learner-centered funding reform)

## Label as 0 (not H3-relevant) if the organization fits any of the following:

**Traditional schools and programs without H3 orientation**
- Schools, charter networks, or districts that serve underserved communities with high-quality, rigorous, college-preparatory education — but whose model is defined by grade-level cohorts, standardized curriculum, and seat-time progression. Equity mission and academic excellence alone do not make a school H3.
- Out-of-school, after-school, enrichment, or extracurricular programs — including STEM, arts, robotics, coding clubs, and cultural programs — even when they use hands-on or project-based methods. These must be the primary school model, not a supplemental program, to qualify.
- Programs that supplement traditional schooling with tutoring, mentoring, college access advising, or career readiness — even when personalized and equity-focused. Supporting students within the traditional system is not the same as redesigning the system.
- Language immersion, arts integration, or STEAM-focused schools whose primary innovation is the content focus rather than the learning model itself.

**Support and wraparound services**
- Mental health, wellness, or behavioral health services for students, even when school-based or tech-enabled, unless the primary mission is redesigning how learning is delivered.
- Family literacy, attendance nudge programs, or family engagement platforms — even when they build caregiver agency — unless they fundamentally reshape how learning is co-designed with families.
- Workforce development, job training, career navigation, or college access programs that help students succeed within the existing system rather than redesigning it.
- Organizations serving specific student populations (military families, students with disabilities, at-risk youth) through support services rather than through an alternative learning model.

**Operational and administrative infrastructure**
- Scholarship platforms, education savings accounts, or school choice navigation tools that expand access to existing options without advancing H3 learning models specifically.
- General educator professional development, new teacher coaching, or mentoring programs not explicitly oriented toward learner-centered pedagogy.
- School board training, district leadership development, or governance support not explicitly tied to H3 school redesign.
- Collective impact networks, cradle-to-career coalitions, or community education initiatives focused on improving outcomes within the traditional system.
- Awareness campaigns, advocacy coalitions, or policy organizations focused on equity, funding, or access — unless the specific policy goal is enabling H3 practices.

## Decision heuristic:
Ask two questions: (1) Does this organization redesign *how* learning happens — shifting power and agency toward the learner, replacing seat-time with demonstrated mastery, or building the infrastructure for that shift? (2) Is that redesign the *primary* mission, not a feature or byproduct of serving students well?

If both answers are yes → 1. If the organization serves students well, advances equity, or improves outcomes *within* the traditional paradigm → 0.

**The key distinction:** An org can be excellent, equity-focused, and deeply committed to student success and still be 0. H3 is about reimagining the *model* of learning, not about the quality or mission of the organization.

## Output format:
Respond with a single integer — 0 or 1 — and nothing else.

## Examples:

Description: "Edlink specializes in integration solutions for e-learning companies and publishers, addressing single sign-on, content integration, grade pass-back, and course rostering across learning management systems and student information systems."
1

Description: "America Succeeds is a nonprofit that engages business leaders to modernize education systems. It promotes 'durable skills' such as critical thinking, collaboration, and adaptability, and works to transform the school-to-work pipeline."
1

Description: "Elevate K-12 provides live teaching solutions to address teacher vacancies and expand course offerings in school districts across the United States, covering core subjects, electives, and special education."
1

Description: "The Forest School is a micro-school serving students from pre-kindergarten through 12th grade, part of the Acton Academy network. It emphasizes learner-driven education, focusing on the development of character, curiosity, and independence."
1

Description: "Green Dot Public Schools is a public charter school organization founded to transform public education in historically underserved neighborhoods. It operates schools serving students in grades 6-12, focusing on preparing all scholars for college, leadership, and life, emphasizing equitable learning opportunities."
0

Description: "OneGoal addresses the opportunity gap in postsecondary education for students from low-income communities, transforming postsecondary advising and support to ensure every student has equitable access to achieving their postsecondary goals."
0

Description: "Long Beach BLAST is a nonprofit focused on supporting at-promise youth through academic and personal mentorship programs, addressing challenges faced by children living in poverty, aiming to reduce dropout rates and improve higher education enrollment."
0

Description: "VirginiaFIRST is a nonprofit providing mentor-based robotics and STEM education programs to young people, engaging students from elementary through high school in competitive robotics programs that build STEM skills and critical thinking."
0

Description: "The New Teacher Center focuses on enhancing the effectiveness of mentoring and coaching to improve educational outcomes for teachers and students, supporting new teachers in their transition to the classroom and fostering conditions that promote well-being and long-term career satisfaction."
0

Description: "Villa Musica is a nonprofit providing music instruction — private lessons, group classes, and community ensembles — to individuals of all ages, with partnerships with local schools and libraries."
0

Description: "Compass Family Services assists families experiencing homelessness through personalized engagements aimed at stable housing, emotional health, and economic self-sufficiency."
0

## Description:
\n
"""
).strip()

def get_bulk_relevancy(
    ids_texts: list[tuple],
    use_cached_results: bool = True,
    prompt_string: str = BASE_PROMPT,
    n_per_commit: int = 50,
    max_workers: int = 10,
    max_errors: int = 1,
    prompt_name: str = "education_relevance",
):
    with get_session() as session:
        prompt_response = PromptResponseCacheSQL(
            session=session,
            prompt_str=prompt_string,
            prompt_name=prompt_name,
        )

        ids_to_response = prompt_response.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_results,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
        )
    return {k: v["response_text"] for k, v in ids_to_response.items()}
