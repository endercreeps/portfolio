export type Language = 'fr' | 'en';

export interface NavLink {
  id: string;
  label: string;
}

export interface HeroContent {
  badge: string;
  title: string;
  subtitle: string;
  ctaContact: string;
  ctaProjects: string;
  ctaCv: string;
}

export interface AboutContent {
  title: string;
  paragraphs: string[];
}

export interface ProjectItem {
  title: string;
  summary: string;
  stack: string[];
  links: {
    demo?: string;
    repository?: string;
  };
}

export interface SkillGroup {
  category: string;
  items: string[];
}

export interface ExperienceItem {
  role: string;
  company: string;
  period: string;
  details: string[];
}

export interface SchoolItem {
  diplome: string;
  school: string;
  period: string;
  actual: boolean;
}

export interface ContactContent {
  title: string;
  intro: string;
  emailLabel: string;
  linkedinLabel: string;
  githubLabel: string;
  locationLabel: string;
  email: string;
  linkedin: string;
  github: string;
  location: string;
}

export interface SectionTitles {
  about: string;
  projects: string;
  skills: string;
  experience: string;
  school: string;
  contact: string;
}

export interface Labels {
  inProgress: string;
}

export interface FooterContent {
  rights: string;
}

export interface PortfolioContent {
  languageLabel: string;
  themeLabel: string;
  nav: NavLink[];
  hero: HeroContent;
  sectionTitles: SectionTitles;
  labels: Labels;
  about: AboutContent;
  projects: ProjectItem[];
  skills: SkillGroup[];
  experiences: ExperienceItem[];
  school: SchoolItem[];
  contact: ContactContent;
  footer: FooterContent;
}
