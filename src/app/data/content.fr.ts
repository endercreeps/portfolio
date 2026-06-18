import { PortfolioContent } from '../models/content.models';

export const CONTENT_FR: PortfolioContent = {
  languageLabel: 'Langue',
  themeLabel: 'Theme',
  nav: [
    { id: 'hero', label: 'Accueil' },
    { id: 'about', label: 'À propos' },
    { id: 'interests', label: "Centres d'intérêt" },
    { id: 'projects', label: 'Projets' },
    { id: 'skills', label: 'Compétences' },
    { id: 'experience', label: 'Expériences' },
    { id: 'school', label: 'Études' },
    { id: 'contact', label: 'Contact' }
  ],
  hero: {
    badge: 'Corentin PHILYS',
    title: 'Développeur Full Stack orienté développement applicatif',
    subtitle:
      'Je conçois et implémente des applications web, mobile, *desktop* et native.',
    ctaContact: 'Me contacter',
    ctaProjects: 'Voir mes projets',
    ctaCv: 'Télécharger mon CV'
  },
  sectionTitles: {
    about: 'À propos',
    interests: "Centres d'intérêt",
    projects: 'Projets',
    skills: 'Compétences',
    experience: 'Expérience',
    school: "Études",
    contact: 'Contact'
  },
  labels: {
    inProgress: 'Diplôme en cours'
  },
  about: {
  title: 'Profil',
  paragraphs: [
    "Étudiant en développement applicatif, je me spécialise dans la conception d'applications interactives et d'interfaces modernes.",
    "J'ai acquis une expérience concrète à travers le développement d'applications pour bornes interactives connectées, en utilisant notamment Angular pour les interfaces web ainsi que C# pour les différents services d'un *middleware* mais aussi via des projets en React Native, Java/Kotlin (Android) et Rust.",
    "Je m'intéresse particulièrement à la qualité du code et à la création de solutions robustes intégrées dans des environnements réels.",
    "Je recherche une alternance me permettant de renforcer mes compétences en développement web et applicatif au sein d'une équipe technique où je pourrai contribuer activement à des projets concrets.",
    "Curieux, autonome et orienté résolution de problèmes, j'apporte une capacité d'adaptation rapide, une approche analytique et une réelle motivation à apprendre et à produire des solutions fiables et maintenables."
  ]
},
  interests: [
    'Programmation',
    'L0AD (Laboratoire ouvert Drôme Ardèche)',
    "Nuit de l'Info",
    'Sport (Musculation depuis 4 ans)',
    'Sport automobile',
    'Jeu de société',
    'Culture japonaise'
  ],
  projects: [
    {
      title: 'Space Shooter',
      summary: "Jeu type *Space Shooter* avec le moteur de jeu Godot dans le cadre d'un projet universitaire.\n__Objectif :__ Menu 3D dans l'espace avec choix de niveaux, jeu en 2D avec moteur physique. Implémentation de différentes fonctionnalités pertinentes pour un jeu.\n__Réalisation :__ Différents modes de jeu, intégration d'un *HUD* 3D dans un jeu en 2D, fichier de configuration pour les paramètres, les scores et les niveaux",
      stack: ['Godot', 'GdScript', 'Git'],
      links: {
      }
    },
    {
      title: 'Aramis',
      summary: "Application mobile et interface superviseur web dans le cadre d'un projet universitaire pour une entreprise.\n__Besoin :__ application pour aider les préparateurs à réaliser les commandes de manière efficace et administration sur un panel web.\n__Réalisation :__ Suivi des préparations en temps réel, visualisation de palette en 3D, algorithme de tri intelligent, reconnaissance vocal.",
      stack: ['Kotlin', 'API REST', 'PostgreSQL', 'Android SDK', 'Django', 'Filament', 'Vosk', 'Tests unitaires', 'Gitlab CI/CD'],
      links: {
      }
    },
    {
      title: 'Portfolio',
      summary: "Portfolio personnel pour présenter mes projets et compétences.\n__Objectif :__ Présenter mon profil à travers un site vitrine\n__Réalisation :__ Site web responsive avec Angular, intégration de contenu dynamique, mise en place d'une architecture modulaire et d'un système de navigation fluide.",
      stack: ['Angular', 'Typescript', 'HTML/SCSS'],
      links: {
        repository: "https://github.com/endercreeps/portfolio/"
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
      category: 'Applicatif',
      items: ['Python', 'C/C++', 'C#', 'Kotlin', 'Java', 'Rust']
    },
    {
      category: 'Mobile',
      items: ['Flutter', 'Kotlin', 'Java', 'Android SDK', 'React Native']
    },
    {
      category: 'Environnements',
      items: ['VirtualBox', 'WSL', 'Gitlab CI/CD', 'Docker Compose']
    },
    {
      category: "Systèmes d'exploitation",
      items: ['Linux', 'Windows', 'Android']
    }
  ],
  experiences: [
    {
      role: '[Alternance] Développement logiciel de bornes interactives',
      company: 'IPM France',
      period: '09/2025 - 08/2026',
      details: [
        "Travail d'équipe, formalisation du besoin client",
        "Optimisation d'une application web complexe (Angular)",
        "Réalisation d'un service pour middleware propriétaire pour l'utilisation d'un lecteur Vitale (PAX, Vit@Jour) en C#.",
        "*Refactoring* d'un service de lecture de carte Vitale via lecteur PC/SC",
        "Outillage : POC console, mise en place serveur Gitlab/Gitea, chaine de compilation et plugins Jenkins/Gitlab"
      ]
    },
    {
      role: '[Stage] Développement logiciel de bornes interactives',
      company: 'IPM France',
      period: '03/2025 - 06/2025',
      details: [
        "Travail d'équipe, formalisation du besoin client",
        "Optimisation d'une application web complexe (Angular) avec correction de bugs, ajout d'amélioration et migration Angular8 à Angular19 ",
        "Création et optimisations d'applications web de démonstration (Angular)"
      ]
    }
  ],
  school: [
    {
      diplome: "[MASTER] Informatique (Parcours ILSEN)",
      school: "Université d'Avignon, Avignon, Vaucluse (84)",
      period: '09/2026 - 07/2028',
      actual: true,
    },
    {
      diplome: "[BUT] Informatique (Parcours réalisation et développement d'application)",
      school: 'IUT de Valence, Valence, Drôme (26)',
      period: '09/2023 - 07/2026',
      actual: false,
    },
    {
      diplome: '[BAC] Général (Mathématiques, Numérique et Science Informatiques, Physique-Chimie) **Mention Bien**',
      school: 'Lycée Édouard Branly, Lyon 05, Rhône (69)',
      period: '09/2021 - 07/2023',
      actual: false,
    }
  ],
  contact: {
    title: 'Échangeons sur votre besoin',
    intro:
      "Je suis ouvert aux opportunités d'alternance en développement applicatif.",
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
    rights: '© 2026 Corentin PHILYS - Tous droits réservés.'
  }
};
