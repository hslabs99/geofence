import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import AppChrome from "@/components/AppChrome";
import AuthGuard from "@/components/AuthGuard";
import { ViewModeProvider } from "@/contexts/ViewModeContext";
import { SummaryHistoryProvider } from "@/contexts/SummaryHistoryContext";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Geofence",
  description: "VWork jobs viewer",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <ViewModeProvider>
          <SummaryHistoryProvider>
            <AuthGuard>
              <AppChrome>{children}</AppChrome>
            </AuthGuard>
          </SummaryHistoryProvider>
        </ViewModeProvider>
      </body>
    </html>
  );
}
