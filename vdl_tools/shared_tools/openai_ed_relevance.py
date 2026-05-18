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

# ---------------------------------------------------------------------------
# H3 prompt bodies (no examples) — combine with H3_EXAMPLES at call time
# ---------------------------------------------------------------------------

# V3: decision-first architecture — core question and heuristic up front,
# condensed criteria, "false positive" framing for negatives, structured CoT.
H3_PROMPT_BODY_V3 = dedent(
    """You are a binary classifier. Determine whether an organization is **Future of Learning** (H3) aligned.

## The core question
Does this organization's **primary mission** redesign **how** learning happens — moving away from traditional, seat-time schooling toward learner-centered, competency-based, mastery-driven models?

**The key insight:** An org can be excellent, equity-focused, and deeply committed to student success and still be 0. The signal is in the **learning model**, not the mission quality or the population served.

## Decision test — apply this before reading the criteria
1. What is the primary learning model this org uses or builds? Is it fundamentally different from traditional seat-time schooling?
2. Is redesigning the learning model the **primary** mission — not a feature, byproduct, or one of many programs?

If both yes → 1. If the org serves students well within the traditional paradigm → 0.

---

## Label 1 — Future of Learning aligned

**Redesigned learning models (schools and platforms)**
- Students advance on demonstrated mastery, not seat time or grade level
- Genuinely self-directed, self-paced, or learner-co-designed learning
- Alternative school structures: micro-schools, Montessori, Waldorf, Acton, Big Picture, democratic schools, multi-age environments
- Virtual/hybrid models where the *learning model itself* is redesigned around mastery and agency — not a traditional school delivered on a screen

**Infrastructure that exists specifically for H3 learning**
- Mastery transcripts, competency-based credentials, digital portfolios that replace GPA signals
- Adaptive/personalized platforms that change *what and how* a learner engages — not just pace through the same curriculum
- CBE policy advocacy, competency framework development, Portrait of a Graduate initiatives
- School incubators, accelerators, or field-building orgs explicitly scaling new learning models

**Educator and system transformation toward new models**
- PD explicitly reorienting pedagogy: mastery-based grading, self-paced learning, learner-centered facilitation
- Staffing or certification reform that redefines the educator role to enable a new learning model
- Research or advocacy whose specific goal is enabling H3 practices (CBE policy, alternative credentialing, mastery-based funding)

---

## Label 0 — common false positives (look H3 but are not)

**Excellent schools working within the traditional model**
- High-quality, college-prep, rigorous charter or district schools defined by grade-level cohorts + standardized curriculum + seat-time progression. Equity mission and academic excellence are not H3 signals.
- Content-focused schools (STEAM, language immersion, arts integration) whose innovation is *what* is taught, not *how* learning is structured.

**Supplemental programs — even hands-on and project-based**
- After-school, out-of-school, enrichment, or extracurricular programs (robotics, coding, STEM clubs, makerspaces) — regardless of pedagogy, these supplement the traditional system.
- Tutoring, mentoring, college access, career readiness, or advising — supporting students to succeed *within* the traditional system.

**Tools serving the traditional system**
- EdTech that automates or streamlines teacher or admin workflows: auto-grading for teacher efficiency, scheduling, attendance, SIS, rostering, LMS — even if AI-powered. Exception: auto-grading used to deliver *continuous mastery feedback to learners* (not just save teacher time) may qualify.
- Connectivity, hardware, or general classroom tools that improve how the existing model runs — not how learning is structured.

**Support and wraparound services**
- Mental health, wellness, family engagement, attendance, or wraparound services — even school-embedded and tech-enabled.
- Workforce development, job training, or postsecondary navigation helping students succeed in the existing system.

---

## Output format:
Respond with JSON in this exact field order:
1. "reasoning": In 1-2 sentences, answer both decision questions — (1) what is the primary learning model, and (2) is redesigning learning the primary mission? Then state your conclusion.
2. "label": 0 or 1"""
).strip()

# Strict heuristic: redesign must be the *primary* mission
H3_PROMPT_BODY = dedent(
    """You are a binary relevance classifier for the LearnerStudio Education Ecosystem.

Your task is to determine whether an organization is aligned with Horizon 3 (H3) education — a visionary, future-ready learning paradigm that shifts away from traditional, standardized schooling toward student-driven, competency-based mastery. H3 emphasizes equity, personalized learning, real-world application, learner agency, creativity, and critical thinking, preparing students to thrive in an unpredictable future.

The organizations you will classify span the full education ecosystem. H3 alignment can appear in any sector or org type — it is about the orientation and philosophy of the work, not the category alone.

## Label as 1 (H3-relevant) if the organization meaningfully advances any of the following:

**Learner-centered models and pedagogy**
- Competency-based, mastery-based, or proficiency-based learning where students advance upon demonstrated mastery, not seat time
- Project-based, experiential, or inquiry-driven learning where the project or experience is the primary vehicle for knowledge-building — not a supplemental activity
- Personalized or adaptive learning models that tailor pace, path, or content to individual learners at a systemic level
- Alternative school models: micro-schools, Montessori, Waldorf, Reggio Emilia, democratic schools, multi-age learning environments
- Virtual, hybrid, or location-independent models where the *learning model itself* is redesigned around learner agency and mastery — not simply a traditional school delivered via a screen
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
- AI, edtech, or software tools whose primary purpose is to automate or streamline *administrative* workflows within the traditional model — such as grading handwritten homework for teacher efficiency, scheduling, attendance tracking, or compliance reporting — even when powered by machine learning. Note: tools that use AI or auto-grading as a mechanism to deliver *continuous mastery feedback to learners* (not just save teacher time) may qualify under "embedded formative and mastery assessment tools" above.
- Online or virtual schools that replicate a traditional grade-level, seat-time curriculum in a digital environment — the delivery medium is not the learning model.

## Decision heuristic:
Ask two questions: (1) Does this organization redesign *how* learning happens — shifting power and agency toward the learner, replacing seat-time with demonstrated mastery, or building the infrastructure for that shift? (2) Is that redesign the *primary* mission, not a feature or byproduct of serving students well?

If both answers are yes → 1. If the organization serves students well, advances equity, or improves outcomes *within* the traditional paradigm → 0.

**The key distinction:** An org can be excellent, equity-focused, and deeply committed to student success and still be 0. H3 is about reimagining the *model* of learning, not about the quality or mission of the organization.

## Output format:
Respond with JSON in this exact field order:
1. "reasoning": 1-2 sentences identifying the key signals that determine your classification
2. "label": 0 or 1"""
).strip()

# Softened heuristic: H3 contribution need not be the *only* focus
H3_PROMPT_V2_BODY = dedent(
    """You are a binary relevance classifier for the LearnerStudio Education Ecosystem.

Your task is to determine whether an organization is aligned with Horizon 3 (H3) education — a visionary, future-ready learning paradigm that shifts away from traditional, standardized schooling toward student-driven, competency-based mastery. H3 emphasizes equity, personalized learning, real-world application, learner agency, creativity, and critical thinking, preparing students to thrive in an unpredictable future.

The organizations you will classify span the full education ecosystem. H3 alignment can appear in any sector or org type — it is about the orientation and philosophy of the work, not the category alone.

## Label as 1 (H3-relevant) if the organization meaningfully advances any of the following:

**Learner-centered models and pedagogy**
- Competency-based, mastery-based, or proficiency-based learning where students advance upon demonstrated mastery, not seat time
- Project-based, experiential, or inquiry-driven learning where the project or experience is the primary vehicle for knowledge-building — not a supplemental activity
- Personalized or adaptive learning models that tailor pace, path, or content to individual learners at a systemic level
- Alternative school models: micro-schools, Montessori, Waldorf, Reggio Emilia, democratic schools, multi-age learning environments
- Virtual, hybrid, or location-independent models where the *learning model itself* is redesigned around learner agency and mastery — not simply a traditional school delivered via a screen
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
- AI, edtech, or software tools whose primary purpose is to automate or streamline *administrative* workflows within the traditional model — such as grading handwritten homework for teacher efficiency, scheduling, attendance tracking, or compliance reporting — even when powered by machine learning. Note: tools that use AI or auto-grading as a mechanism to deliver *continuous mastery feedback to learners* (not just save teacher time) may qualify under "embedded formative and mastery assessment tools" above.
- Online or virtual schools that replicate a traditional grade-level, seat-time curriculum in a digital environment — the delivery medium is not the learning model.

## Decision heuristic:
Ask: Does this organization meaningfully contribute to H3 education — by redesigning how learning happens, building tools or infrastructure that enable it, or driving ecosystem conditions for it to scale? If yes → 1, even if H3 is not their *only* focus. Serving students well *within* the traditional paradigm, without meaningfully advancing any H3 dimension → 0.

**The key distinction:** An org can be excellent, equity-focused, and deeply committed to student success and still be 0. H3 is about advancing alternatives to the traditional model — not about the quality or mission of the organization. An org that does H3-aligned work *alongside* other activities still qualifies if the H3 contribution is genuine and meaningful, not merely incidental.

## Output format:
Respond with JSON in this exact field order:
1. "reasoning": 1-2 sentences identifying the key signals that determine your classification
2. "label": 0 or 1"""
).strip()

# ---------------------------------------------------------------------------
# Shared examples — 11 positives + 10 negatives, interleaved.
# All descriptions are taken verbatim from the training/test dataset records.
# IDs are noted in comments for traceability; do not modify descriptions here.
# Update this one place; all prompts pick it up automatically.
# ---------------------------------------------------------------------------
H3_EXAMPLES = dedent("""
## Examples:

Description: "Edlink specializes in facilitating seamless integration solutions for e-learning companies and publishers, addressing key challenges such as single sign-on, content integration, grade pass-back, and course rostering. By supporting a diverse range of learning management systems, student information systems, identity management solutions, and data providers, Edlink aims to streamline the integration process, making it efficient and user-friendly. Their focus is on enhancing connectivity within the education sector to improve the overall learning experience. Founded in 2020 and based in Austin, Texas, Edlink is committed to advancing educational technology through innovative solutions."
1

Description: "Elevate K-12 is an educational organization that provides live teaching solutions to address teacher vacancies and enhance the quality of education in various school districts across the United States. The organization offers a range of live courses for elementary, middle, and high school students, covering core subjects, electives, and special education. Elevate K-12 aims to support districts in filling hard-to-fill roles, expanding course offerings, and providing specialized attention to meet diverse student needs. The platform is designed to create an engaging classroom environment where students interact with qualified educators in real-time, fostering positive relationships and effective learning experiences. Elevate K-12 has partnered with over 600 school districts, receiving positive feedback from educators and students regarding the quality of instruction and support provided."
0

Description: "The Forest School, located in Trilith, Fayetteville, GA, is a micro-school that serves students from pre-kindergarten through 12th grade. Established in 2018, it is part of the Acton Academy network, which emphasizes learner-driven education. The school aims to cultivate a holistic approach to education, focusing on the development of character, curiosity, and independence in learners. The Forest School employs a unique educational model that includes Socratic discussions, hands-on projects, and real-world apprenticeships, fostering a diverse and collaborative learning environment. Students are encouraged to take ownership of their education, setting personal goals and engaging in meaningful work. The school operates without traditional grades, instead utilizing portfolios and exhibitions to showcase student progress."
1

Description: "Green Dot Public Schools National is dedicated to transforming public education to ensure that every student is equipped for success in college, leadership, and life. The organization manages and supports the operation of public charter schools across various regions, including California and Tennessee, focusing on middle and high school education. Green Dot provides essential services such as curriculum design, staff training, and operational support, while also securing philanthropic funding and facilities for its schools. Through these efforts, Green Dot aims to foster educational environments that empower students and their families."
0

Description: "Khan Academy is dedicated to providing a free, world-class education accessible to anyone, anywhere. The organization offers a comprehensive platform featuring a vast array of educational resources, including instructional videos, practice exercises, and articles, primarily focused on K-12 subjects with an emphasis on math and science. Through partnerships with schools and educational institutions globally, Khan Academy aims to enhance learning outcomes, particularly in historically under-resourced communities. With a commitment to continuous improvement, the organization is expanding its content library and platform capabilities to ensure personalized learning experiences for millions of users worldwide. Khan Academy's mission is to empower learners of all ages to achieve their educational goals and foster a lifelong passion for learning."
1

Description: "OneGoal is an organization dedicated to addressing the opportunity gap in education, particularly for students from low-income communities. Its vision is to ensure that every student has equitable access to achieve their postsecondary aspirations. The organization focuses on transforming postsecondary advising and support through various programs, including the OneGoal Program, OneGoal Leadership Network, and OneGoal Essentials. Over its 15 years of operation, OneGoal has impacted more than 150,000 students by empowering educators and students to navigate the postsecondary landscape effectively."
0

Description: "The Modern Classrooms Project is an educational organization focused on transforming traditional teaching methods to better meet the diverse needs of students. Founded in 2018 by former math teachers Kareem Farah and Robert Barnett, the organization promotes a research-backed instructional model that emphasizes blended instruction, self-paced learning, and mastery-based assessment. This approach allows students to learn at their own pace, ensuring they grasp key concepts before moving on to more advanced material. The organization provides resources and training for educators worldwide, aiming to eliminate one-size-fits-all teaching practices. By encouraging teachers to create instructional videos and implement flexible learning structures, the Modern Classrooms Project seeks to enhance both student engagement and teacher satisfaction."
1

Description: "FIRST Chesapeake, also known as VirginiaFIRST, is dedicated to inspiring youth in the District of Columbia, Maryland, and Virginia to become leaders in science and technology. Through engaging, mentor-based programs, the organization fosters essential STEM skills while promoting creativity, innovation, and entrepreneurship. FIRST Chesapeake administers a variety of robotics competitions, including the FIRST Robotics Challenge and FIRST Tech Challenge, impacting thousands of students annually. By collaborating with partners like the FIRST LEGO League, the organization ensures a comprehensive continuum of STEM-based leadership opportunities for students from kindergarten through high school."
0

Description: "Authentica Solutions is an organization focused on enhancing the educational landscape through innovative solutions and products. Their primary offering, the Authentica seedTM platform, is a Software as a Service (SaaS) solution designed to improve data management and interoperability within educational institutions. This platform aims to ensure that decisions are based on reliable and accessible data, thereby accelerating the flow of learning globally. The organization provides tailored services, including managed and professional services, to meet the diverse needs of K12 school districts, higher education institutions, EdTech software providers, system integrators, and strategic partners."
1

Description: "The New Teacher Center (NTC) is a nonprofit organization focused on improving the effectiveness of educators and enhancing student learning outcomes through targeted mentoring and coaching. Established over 25 years ago, NTC aims to create supportive environments for new teachers, helping them navigate the challenges of the profession and fostering their development into effective educators. The organization partners with schools and districts across the United States, including urban and rural communities, to implement evidence-based professional learning strategies that are tailored to the specific needs of educators. NTC's approach emphasizes the importance of building strong, trust-based relationships between coaches and teachers, which is essential for effective instructional coaching."
0

Description: "Parchment is a digital credential management platform that facilitates the secure issuance, exchange, and verification of academic and professional credentials such as transcripts, diplomas, digital badges, certificates, and verifications. Serving K-12 schools, higher education institutions, state agencies, employers, and professional organizations, Parchment centralizes and digitizes credential workflows to support student mobility, enrollment, transfer processes, and workforce credentialing. The platform offers solutions for records digitization, transcript management, and credential verification, emphasizing privacy, security, and compliance with standards like FERPA, SOC 2 Type 2, and PCI."
1

Description: "Bakpax develops AI-powered solutions that read handwritten student assignments and automatically grade them, streamlining the evaluation process for educators. Their platform converts handwritten content into formatted, interactive digital text, enabling teachers to efficiently monitor student performance and provide timely feedback. By leveraging widely accessible tools like paper and smartphones, Bakpax aims to enhance educational technology without requiring costly hardware or complex software. This approach supports equitable access to learning resources while fostering greater student engagement and improved classroom insights."
0

Description: "Big Picture Learning is an organization focused on transforming education to be more student-centered, supporting students in designing their own lives through mentorship and diverse learning opportunities. It operates a global network of over 275 schools and learning sites where community-based, experiential learning is emphasized alongside traditional classroom instruction. The organization also functions as an incubator for innovative educational practices, offering fellowships, partnerships, and initiatives that extend learning beyond school walls. With a history spanning 30 years, Big Picture Learning engages in research, advocacy, and leadership to influence educational policy and practice."
1

Description: "Communities In Schools of Kalamazoo (CIS) is dedicated to supporting students in their educational journeys by providing a network of resources and caring adults. The organization operates within Kalamazoo Public Schools, focusing on addressing the barriers that students face in achieving academic success. CIS employs a model that emphasizes one-on-one relationships between students and site coordinators, who assess individual needs and coordinate support services. The organization connects students with various resources, including academic assistance, health services, and after-school programs, aiming to foster a safe and nurturing environment. CIS has demonstrated significant impact through its programs, with high rates of student retention, promotion, and graduation."
0

Description: "America Succeeds is a nonprofit organization focused on engaging business leaders to modernize education systems in order to promote equity and opportunity for all students. Founded in 2014, the organization aims to transform the school-to-work pipeline, ensuring that students are equipped with the necessary skills to thrive in a competitive global economy. America Succeeds emphasizes the importance of developing "durable skills," such as critical thinking, collaboration, and adaptability, which are essential for success in both education and the workforce. The organization operates through a combination of advocacy, policy development, and research initiatives, fostering partnerships between the business and education sectors."
1

Description: "The Spartanburg Academic Movement (SAM) is a nonprofit organization and community initiative focused on enhancing educational outcomes and economic mobility for children and families in Spartanburg County, South Carolina. Established in 2013, SAM operates through collaborative partnerships among various stakeholders, including education, business, government, and community leaders. The organization emphasizes data-driven strategies to identify and address achievement gaps across key educational milestones, such as kindergarten readiness, third-grade reading, eighth-grade math, high school graduation rates, and postsecondary education."
0

Description: "The Clayton Christensen Institute is a nonprofit, nonpartisan research organization dedicated to advancing understanding and application of innovative theories to address complex societal challenges. Founded on the principles of the late Harvard Business School professor Clayton Christensen, the Institute focuses on four primary sectors: education, health care, global prosperity, and emerging industries. Through rigorous, theory-based research, the Institute aims to empower policymakers, entrepreneurs, and leaders to foster meaningful progress and systemic change. The organization employs various theoretical frameworks, including Disruptive Innovation, Jobs to be Done, and Modularity, to analyze and propose solutions for persistent issues within these sectors."
1

Description: "BirdBrain Technologies develops educational robotics and STEM tools designed for students from kindergarten through 12th grade. Their product line includes the Hummingbird Robotics Kit for grades 4-12, the Finch robot for grades K-12, the Owlet math tools for grades K-5, and an Electric Motors Catalyst Kit for grades 3-6. These products are created to support hands-on, project-based learning in robotics, computer science, engineering, and mathematics. Developed at Carnegie Mellon University's Robotics Institute, the tools have undergone extensive testing in various educational settings and are supported by National Science Foundation-funded research. BirdBrain Technologies provides free instructional materials, including tutorials, printables, sample code, and teacher training courses, to facilitate integration across diverse subjects and skill levels. Their resources emphasize inclusivity and aim to engage a broad range of students, including those who may not traditionally be drawn to STEM fields. The organization focuses on fostering creativity, collaboration, and confidence through physical computing and innovative curriculum design."
0

Description: "Synthesis is an educational organization that offers a personalized math tutoring platform designed for children aged 5 to 11. The platform aims to enhance students' understanding and confidence in mathematics through an adaptive learning experience that tailors instruction to individual needs. Synthesis Tutor provides immediate feedback and engages students with a multisensory approach, making math learning interactive and enjoyable. The curriculum covers foundational K-5 math concepts while encouraging deeper exploration and problem-solving skills. Synthesis emphasizes the importance of personalized education, aiming to foster a love for learning and critical thinking in children. The organization originated from an experimental school initiative at SpaceX, focusing on innovative learning experiences that prepare students for real-world challenges."
1

Description: "This organization focuses on enhancing student learning by integrating advanced digital curriculum and instruction with local school resources. Their educational model supports personalized learning, allowing students to progress academically at their own pace with guidance from teaching staff. The approach aims to prepare students to become lifelong learners and productive citizens. A key objective is to help all students successfully earn a high school diploma through this blended learning environment."
0

Description: "InnovateEDU is a nonprofit organization focused on accelerating the transformation of education ecosystems by addressing systemic challenges through policy, practice, technology, and research. It aims to create an equitable and inclusive learning environment where all students have access to opportunities and resources that prepare them to succeed globally. Their work spans multiple areas including education technology and markets, sector and capacity building focused on educator development and data-driven decision-making, and policy advocacy to foster large-scale systems change. Key projects include afterschool programs, alliances promoting research and development infrastructure, data and technology communities of practice, coalitions supporting AI safety in education, advocacy for students with disabilities, and efforts to improve data interoperability within K-12 education."
1

Description: "The Aurora Institute is a nonprofit organization dedicated to transforming K-12 education systems through the advancement of student-centered, competency-based learning models. Its mission focuses on driving systemic change to ensure high-quality, equitable learning experiences that prepare all students for college, career, and civic life. The Institute emphasizes personalized learning approaches that empower students to make decisions about their learning, progress based on mastery rather than seat time, and receive timely, differentiated support tailored to their individual needs. The organization operates several strategic initiatives, including a Center for Policy that advocates for student-centered education policies at state and federal levels, CompetencyWorks which shares best practices and knowledge about competency-based education, and a Research and Evaluation team that conducts rigorous studies to build evidence supporting education innovation."
1

Description: "Education Reimagined is an organization focused on transforming the public education system in the United States to be learner-centered, equitable, and socially just. It promotes a vision where every child is recognized as unique and supported to reach their full potential. The organization works by connecting and supporting a diverse community of education leaders, educators, families, and communities who are innovating new learning environments that prioritize the needs and agency of each learner. Education Reimagined emphasizes community-based ecosystems of learning that are personalized, relevant, and adaptable, moving away from traditional age- and seat-time-based structures. It provides resources, virtual courses, and platforms for collaboration to advance learner-centered practices nationwide."
1

## Description:
""").strip()

# ---------------------------------------------------------------------------
# Composed prompts — body + shared examples
# ---------------------------------------------------------------------------

# Strict heuristic + shared examples (used for Runs B, C, D)
H3_PROMPT = H3_PROMPT_BODY + "\n\n" + H3_EXAMPLES

# Softened heuristic + shared examples (used for Run E)
H3_PROMPT_V2 = H3_PROMPT_V2_BODY + "\n\n" + H3_EXAMPLES

# V3: decision-first architecture + shared examples (Run G)
H3_PROMPT_V3 = H3_PROMPT_BODY_V3 + "\n\n" + H3_EXAMPLES

# Fine-tuned model: strict heuristic body only, plain 0/1 output, no examples.
# Training data uses plain "0"/"1" assistant responses, so inference must match.
H3_PROMPT_FT = (
    H3_PROMPT_BODY
    .replace(
        '## Output format:\nRespond with JSON in this exact field order:\n'
        '1. "reasoning": 1-2 sentences identifying the key signals that determine your classification\n'
        '2. "label": 0 or 1',
        "## Output format:\nOutput exactly 0 (not H3-relevant) or 1 (H3-relevant) — no other text.",
    )
    .rstrip() + "\n\n## Description:\n"
)

def get_bulk_relevancy(
    ids_texts: list[tuple],
    use_cached_results: bool = True,
    prompt_string: str = BASE_PROMPT,
    n_per_commit: int = 50,
    max_workers: int = 10,
    max_errors: int = 1,
    prompt_name: str = "education_relevance",
    model: str = "gpt-4.1-mini",
):
    with get_session() as session:
        prompt_response = PromptResponseCacheSQL(
            session=session,
            prompt_str=prompt_string,
            prompt_name=prompt_name,
            model=model,
        )

        ids_to_response = prompt_response.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_results,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
        )
    return {k: v["response_text"] for k, v in ids_to_response.items()}
