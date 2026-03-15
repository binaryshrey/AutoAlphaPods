import { Suspense } from "react";

import CryptoDashboard from "@/components/crypto-dashboard";

export default function DashboardPage() {
  return (
    <Suspense fallback={<div className="p-6">Loading dashboard...</div>}>
      <CryptoDashboard />
    </Suspense>
  );
}
