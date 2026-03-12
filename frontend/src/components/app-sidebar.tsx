"use client";

import {
  BookOpenIcon,
  DatabaseZapIcon,
  InfoIcon,
  LifeBuoyIcon,
  MonitorCogIcon,
  SendIcon,
  Settings2Icon,
  TerminalIcon,
} from "lucide-react";
import type * as React from "react";
import { NavDataset } from "@/components/nav-dataset";
import { NavMain } from "@/components/nav-main";
import { NavSecondary } from "@/components/nav-secondary";
import { NavUser } from "@/components/nav-user";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";

const data = {
  user: {
    name: "IOOS",
    email: "ioos@ioos.us",
    avatar: "/avatars/shadcn.jpg",
  },
  currentDataset: [
    {
      title: "Overview",
      url: "#",
      icon: <InfoIcon />,
    },
    {
      title: "Configuration",
      url: "#",
      icon: <Settings2Icon />,
      isActive: true,
      items: [
        {
          title: "Published",
          url: "#",
        },
        {
          title: "Testing",
          url: "#",
        },
        {
          title: "All Configs",
          url: "#",
        },
      ],
    },
    {
      title: "Processing status",
      url: "#",
      icon: <MonitorCogIcon />,
    },
  ],
  navSecondary: [
    {
      title: "Documentation",
      url: "https://github.com/gulfofmaine/buoy_retriever/",
      icon: <BookOpenIcon />,
    },
    {
      title: "Support",
      url: "https://github.com/gulfofmaine/buoy_retriever/issues",
      icon: <LifeBuoyIcon />,
    },
    {
      title: "Feedback",
      url: "https://github.com/gulfofmaine/buoy_retriever/issues",
      icon: <SendIcon />,
    },
  ],
  navMain: [
    {
      name: "All Datasets",
      url: "/manage/",
      icon: <DatabaseZapIcon />,
    },
    // {
    //   name: "Design Engineering",
    //   url: "#",
    //   icon: (
    //     <FrameIcon
    //     />
    //   ),
    // },
    // {
    //   name: "Sales & Marketing",
    //   url: "#",
    //   icon: (
    //     <PieChartIcon
    //     />
    //   ),
    // },
    // {
    //   name: "Travel",
    //   url: "#",
    //   icon: (
    //     <MapIcon
    //     />
    //   ),
    // },
  ],
};

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  return (
    <Sidebar
      className="top-(--header-height) h-[calc(100svh-var(--header-height))]!"
      {...props}
    >
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild>
              <a href="/">
                <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                  <TerminalIcon className="size-4" />
                </div>
                <div className="grid flex-1 text-left text-sm leading-tight">
                  <span className="truncate font-medium">
                    IOOS Buoy Retriever
                  </span>
                  <span className="truncate text-xs">MetOcean Data Link</span>
                </div>
              </a>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <NavMain projects={data.navMain} />
        <NavDataset items={data.currentDataset} />
        <NavSecondary items={data.navSecondary} className="mt-auto" />
      </SidebarContent>
      <SidebarFooter>
        <NavUser user={data.user} />
      </SidebarFooter>
    </Sidebar>
  );
}
