import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "@/styles/globals.css";
import { AuthProvider } from "@/hooks/useAuth";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "HADES - Plataforma de Trading",
  description: "Señales de trading en tiempo real, análisis de mercado y más",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="es">
      <body className={inter.className}>
        <AuthProvider>
          {children}
        </AuthProvider>
      </body>
    </html>
  );
}
