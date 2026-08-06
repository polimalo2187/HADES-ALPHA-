import Navbar from '@/components/Navbar';

export default function PaymentsLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-950 via-purple-950 to-gray-950">
      <Navbar />
      <main className="pt-16 md:pt-20">
        {children}
      </main>
    </div>
  );
}
