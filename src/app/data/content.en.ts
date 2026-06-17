import { PortfolioContent } from '../models/content.models';

export const CONTENT_EN: PortfolioContent = {
  languageLabel: 'Language',
  themeLabel: 'Theme',
  nav: [
    { id: 'hero', label: 'Home' },
    { id: 'about', label: 'About' },
    { id: 'interests', label: 'Interests' },
    { id: 'projects', label: 'Projects' },
    { id: 'skills', label: 'Skills' },
    { id: 'experience', label: 'Experience' },
    { id: 'school', label: 'Studies' },
    { id: 'contact', label: 'Contact' }
  ],
  hero: {
    badge: 'Corentin PHILYS',
    title: 'Full Stack Developer focused on application development',
    subtitle:
      'I design and build robust, performant and maintainable web, mobile, desktop and native applications.',
    ctaContact: 'Contact me',
    ctaProjects: 'View my projects',
    ctaCv: 'Download my resume'
  },
  sectionTitles: {
    about: 'About',
    interests: 'Interests',
    projects: 'Projects',
    skills: 'Skills',
    experience: 'Experience',
    school: "Studies",
    contact: 'Contact'
  },
  labels: {
    inProgress: "Diploma in progress"
  },
  about: {
    title: 'Profile',
    paragraphs: [
      "As a student of application development, I specialize in designing interactive applications and modern interfaces.",
      "I have gained practical experience through the development of applications for connected interactive kiosks, using Angular for web interfaces and C# for various middleware services, as well as through projects in React Native, Java/Kotlin (Android), and Rust.",
      "I am particularly interested in code quality and creating robust solutions integrated into real-world environments.",
      "I am looking for a work-study program that will allow me to strengthen my web and application development skills within a technical team where I can actively contribute to concrete projects.",
      "Curious, self-motivated, and problem-solving oriented, I bring the ability to adapt quickly, an analytical approach, and a genuine motivation to learn and produce reliable, maintainable solutions."]
  },
  interests: [
    'Programming',
    'L0AD (Laboratoire ouvert Drôme Ardèche)',
    "Nuit de l'Info",
    'Sports (Workout for 4 years)',
    'Motorsports',
    'Board games',
    'Japanese culture',
  ],
  projects: [
    {
      title: 'Space Shooter',
      summary:
        'Space shooter-style game built using the Godot game engine as part of a university project.\n__Objective:__ 3D menu in space with level selection, 2D game with a physics engine. Implementation of various features relevant to a game.\n__Achievements:__ Different game modes, integration of a 3D HUD into a 2D game, configuration file for settings, scores, and levels',
      stack: ['Godot', 'GdScript', 'Git'],
      links: {
      }
    },
    {
      title: 'Aramis',
      summary: 'Mobile app and web-based supervisor interface developed as part of a university project for a company.\n__Requirements:__ An app to help order pickers process orders efficiently, along with a web-based administration panel.\n__Features:__ Real-time order tracking, 3D pallet visualization, intelligent sorting algorithm, voice recognition.',
    stack: ['Kotlin', 'API REST', 'PostgreSQL', 'Android SDK', 'Django', 'Filament', 'Vosk', 'Tests unitaires', 'Gitlab CI/CD'],
      links: {
      }
    }
  ],
  skills: [
    {
      category: 'Frontend',
      items: ['Angular', 'React', 'TypeScript', 'JavaScript', 'HTML', 'CSS/SCSS']
    },
    {
      category: 'Backend',
      items: ['Node.js', 'Express', 'REST API', 'SQL', 'PHP', ' PL/pgsql', 'Django', 'Firebase']
    },
    {
      category: 'Application',
      items: ['Python', 'C/C++', 'C#', 'Kotlin', 'Java', 'Rust']
    },
    {
      category: 'Mobile',
      items: ['Flutter', 'Kotlin', 'Java', 'Android SDK', 'React Native']
    },
    {
      category: 'Environments',
      items: ['VirtualBox', 'WSL', 'Gitlab CI/CD', 'Docker Compose']
    },
    {
      category: "Operating System",
      items: ['Linux', 'Windows', 'Android']
    }
  ],
  experiences: [
    {
      role: '[Apprenticeship] Software development for interactive kiosks',
      company: 'IPM France',
      period: '09/2025 - 08/2026',
      details: [
        "Teamwork, formalizing client requirements",
        "Optimizing a complex web application (Angular)",
        "Development of a service for proprietary middleware to support a Vital reader (PAX, Vit@Jour) in C#",
        "Refactoring of a service that reads Vitale cards using a PC/SC card reader",
        "Tools: Console POC, Gitlab/Gitea server setup, build pipeline and Jenkins/Gitlab plugins"
      ]
    },
    {
      role: '[Internship] Software development for interactive kiosks',
      company: 'IPM France',
      period: '03/2025 - 06/2025',
      details: [
        "Teamwork, defining client requirements",
        "Optimizing a complex web application (Angular) with bugs fixes, new features and Angular8 to Angular19 migration",
        "Creating and optimizing demo web applications (Angular)"
      ]
    }
  ],
  school: [
    {
      diplome: "[Master's degree] IT (ILSEN)",
      school: "Université d'Avignon, Avignon, Vaucluse (84)",
      period: '09/2026 - 07/2028',
      actual: true,
    },
    {
      diplome: "[Bachelor's degree in science and technology] IT ",
      school: 'IUT de Valence, Valence, Drôme (26)',
      period: '09/2023 - 07/2026',
      actual: false,
    },
    {
      diplome: '[High school diploma] General (Mathematics, Computer Science, Physics and Chemistry) **Good grade**',
      school: 'Lycée Édouard Branly, Lyon 05, Rhône (69)',
      period: '09/2021 - 07/2023',
      actual: false,
    }
  ],
  contact: {
    title: 'Let us discuss your needs',
    intro: 'I am open to apprenticeship opportunities in full stack application development.',
    emailLabel: 'Email',
    linkedinLabel: 'LinkedIn',
    githubLabel: 'GitHub',
    locationLabel: 'Localisation',
    email: '',
    linkedin: 'https://www.linkedin.com/in/philysc',
    github: 'https://github.com/endercreeps',
    location: 'Lyon (69) / Avignon (84), France'
  },
  footer: {
    rights: '© 2026 Corentin PHILYS - All rights reserved.'
  }
};
