export default function DatasetLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <div className="md:flex">
      <main className="p-4">{children}</main>
      <aside>Dataset discussion message thread here</aside>
    </div>
  );
}
