import { PortfolioContent } from '../models/content.models';

export const CONTENT_FR: PortfolioContent = {
  languageLabel: 'Langue',
  themeLabel: 'Theme',
  nav: [
    { id: 'hero', label: 'Accueil' },
    { id: 'about', label: 'À propos' },
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
      'Je conçois et implemente des applications web, mobile, *desktop* et native.',
    ctaContact: 'Me contacter',
    ctaProjects: 'Voir mes projets',
    ctaCv: 'Télécharger mon CV'
  },
  sectionTitles: {
    about: 'À propos',
    projects: 'Projets',
    skills: 'Compétences',
    experience: 'Expérience',
    school: "Études",
    contact: 'Contact'
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
  projects: [
    {
      title: 'Space Shooter',
      summary: "Jeu type *Space Shooter* avec le moteur de jeu Godot dans le cadre d'un projet universitaire.\n__Objectif :__ Menu 3D dans l'espace avec choix de niveaux, jeu en 2D avec moteur physique. Implémentation de différentes fonctionnalités pertinente pour un jeu.\n__Réalisation :__ Différents mode de jeu, intégration d'un *HUD* 3D dans un jeu en 2D, fichier de configuration pour les paramètres, les scores et les niveaux",
      stack: ['Godot', 'GdScript', 'Git'],
      links: {
      }
    },
    {
      title: 'Aramis',
      summary: "Application mobile et interface superviseur web dans le cadre d'un projet universitaire pour une entreprise.\n__Besoin :__ application pour aider les préparateurs à réaliser les commandes de manière efficace et administration sur un pannel web.\n__Réalisation :__ Suivi des préparations en temps réel, visualisation de palette en 3D, algorithme de tri intelligent, reconnaisance vocal.",
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
        "Réalisation d'un service pour middleware propriétaire pour l'utilisation d'un lecteur vitale (PAX, Vit@Jour) en C#.",
        "Outillage : POC console, mise en place serveur Gitea, chaine de compilation et plugins Jenkins"
      ]
    },
    {
      role: '[Stage] Développement logiciel de bornes interactives',
      company: 'IPM France',
      period: '03/2025 - 06/2025',
      details: [
        "Travail d'équipe, formalisation du besoin client",
        "Optimisation d'une application web complexe (Angular)",
        "Création et optimisations d'aplications web de démonstration (Angular)"
      ]
    }
  ],
  school: [
    {
      diplome: "[BUT] Informatique (Parcours réalisation et développement d'application)",
      school: 'IUT de Valence, Valence, Drôme (26)',
      period: '09/2023 - 07/2026',
    },
    {
      diplome: '[BAC] Général (Mathématiques, Numérique et Science Informatiques, Physique-Chimie) **Mention Bien**',
      school: 'Lycée Édouard Branly, Lyon 05, Rhône (69)',
      period: '09/2021 - 07/2023',
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
    location: 'Lyon, France'
  },
  footer: {
    rights: '© 2026 Corentin PHILYS - Tous droits réservés.'
  }
};
