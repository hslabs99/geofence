'use client';

import { useCallback, useEffect, useState } from 'react';

const USER_TYPES = ['Super Admin', 'Admin', 'Client'] as const;
type UserType = (typeof USER_TYPES)[number];

type User = {
  userid: string;
  email: string;
  firstname?: string | null;
  surname?: string | null;
  phone?: string | null;
  userType?: string | null;
  customer?: string | null;
  createdAt?: string;
  updatedAt?: string;
};

type ModalState = { mode: 'add' } | { mode: 'edit'; user: User };

const emptyForm = () => ({
  email: '',
  password: '',
  firstname: '',
  surname: '',
  phone: '',
  userType: 'Admin' as UserType,
  customer: '',
});

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modal, setModal] = useState<ModalState | null>(null);
  const [form, setForm] = useState(emptyForm());
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [distinctCustomers, setDistinctCustomers] = useState<string[]>([]);
  const [customersLoading, setCustomersLoading] = useState(false);
  const isAdd = modal?.mode === 'add';
  const editing = modal?.mode === 'edit' ? modal.user : null;

  const loadCustomers = useCallback(() => {
    setCustomersLoading(true);
    fetch('/api/vworkjobs/customers')
      .then((r) => r.json())
      .then((data) => setDistinctCustomers(data.customers ?? []))
      .catch(() => setDistinctCustomers([]))
      .finally(() => setCustomersLoading(false));
  }, []);

  const loadUsers = () => {
    setLoading(true);
    fetch('/api/users')
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setUsers(data.users ?? []);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => loadUsers(), []);
  useEffect(() => loadCustomers(), [loadCustomers]);

  const openAdd = useCallback(() => {
    setSubmitError(null);
    setForm(emptyForm());
    setModal({ mode: 'add' });
  }, []);

  const openEdit = useCallback((u: User) => {
    setSubmitError(null);
    setForm({
      email: u.email,
      password: '',
      firstname: u.firstname ?? '',
      surname: u.surname ?? '',
      phone: u.phone ?? '',
      userType: (USER_TYPES.includes((u.userType ?? 'Admin') as UserType) ? u.userType : 'Admin') as UserType,
      customer: u.customer ?? '',
    });
    setModal({ mode: 'edit', user: u });
  }, []);

  const closeForm = useCallback(() => {
    setModal(null);
    setSubmitError(null);
    setForm(emptyForm());
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitError(null);
    const body: Record<string, string> = {
      email: form.email.trim(),
      firstname: form.firstname.trim() || '',
      surname: form.surname.trim() || '',
      phone: form.phone.trim() || '',
      userType: form.userType,
      customer: form.userType === 'Client' ? form.customer.trim() : '',
    };
    if (!editing) {
      if (!form.password || form.password.length < 6) {
        setSubmitError('Password required, min 6 chars');
        return;
      }
      body.password = form.password;
    } else if (form.password) {
      body.password = form.password;
    }

    const url = editing ? `/api/users/${editing.userid}` : '/api/users';
    const method = editing ? 'PUT' : 'POST';
    fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error ?? r.statusText);
        closeForm();
        loadUsers();
      })
      .catch((e) => setSubmitError(e.message));
  };

  const handleDelete = (u: User) => {
    if (!confirm(`Delete user ${u.email}?`)) return;
    fetch(`/api/users/${u.userid}`, { method: 'DELETE' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error ?? r.statusText);
        loadUsers();
      })
      .catch((e) => setError(e.message));
  };

  return (
    <div className="w-full min-w-0 p-6">
      <div className="mb-4 flex items-center justify-between">
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">User Admin</h1>
        <button
          type="button"
          onClick={openAdd}
          className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
        >
          Add user
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-800 dark:bg-red-900/30 dark:text-red-200">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-zinc-600">Loading…</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Email</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Type</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Customer</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">First name</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Surname</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Phone</th>
                <th className="px-3 py-2 font-medium text-zinc-900 dark:text-zinc-100">Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.userid} className="border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-800/50">
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.email}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.userType ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.customer ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.firstname ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.surname ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-700 dark:text-zinc-300">{u.phone ?? '—'}</td>
                  <td className="whitespace-nowrap px-3 py-2">
                    <button
                      type="button"
                      onClick={() => openEdit(u)}
                      className="mr-2 text-blue-600 hover:underline dark:text-blue-400"
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDelete(u)}
                      className="text-red-600 hover:underline dark:text-red-400"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {users.length === 0 && <p className="p-4 text-zinc-500">No users yet.</p>}
        </div>
      )}

      {modal !== null && (
        <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
            onClick={(e) => { if (e.target === e.currentTarget) closeForm(); }}
          >
          <div
            className="w-full max-w-md rounded-lg bg-white p-6 shadow-lg dark:bg-zinc-900"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="mb-4 text-lg font-semibold text-zinc-900 dark:text-zinc-100">
              {editing ? 'Edit user' : 'Add user'}
            </h2>
            <form onSubmit={handleSubmit} className="flex flex-col gap-3">
              {submitError && (
                <p className="text-sm text-red-600 dark:text-red-400">{submitError}</p>
              )}
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Email *</label>
                <input
                  type="email"
                  value={form.email}
                  onChange={(e) => setForm((f) => ({ ...f, email: e.target.value }))}
                  required
                  disabled={!!editing}
                  autoComplete={isAdd ? 'off' : 'email'}
                  className={`w-full rounded border px-2 py-1.5 text-sm ${isAdd ? 'border-zinc-300 bg-white text-zinc-900' : 'border-zinc-300 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100'} disabled:opacity-60`}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">
                  Password {editing ? '(leave blank to keep)' : '*'}
                </label>
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm((f) => ({ ...f, password: e.target.value }))}
                  minLength={editing ? 0 : 6}
                  placeholder={editing ? '••••••••' : undefined}
                  autoComplete={isAdd ? 'new-password' : 'current-password'}
                  className={`w-full rounded border px-2 py-1.5 text-sm ${isAdd ? 'border-zinc-300 bg-white text-zinc-900' : 'border-zinc-300 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100'}`}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">First name</label>
                <input
                  type="text"
                  value={form.firstname}
                  onChange={(e) => setForm((f) => ({ ...f, firstname: e.target.value }))}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Surname</label>
                <input
                  type="text"
                  value={form.surname}
                  onChange={(e) => setForm((f) => ({ ...f, surname: e.target.value }))}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">Phone</label>
                <input
                  type="text"
                  value={form.phone}
                  onChange={(e) => setForm((f) => ({ ...f, phone: e.target.value }))}
                  className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                />
              </div>
              <div>
                <label className="mb-1 block text-xs font-medium text-zinc-500">User type</label>
                <div className="flex gap-4">
                  {USER_TYPES.map((t) => (
                    <label key={t} className="flex items-center gap-1.5 text-sm">
                      <input
                        type="radio"
                        name="userType"
                        checked={form.userType === t}
                        onChange={() => setForm((f) => ({ ...f, userType: t, customer: t === 'Client' ? f.customer : '' }))}
                        className="rounded border-zinc-300 text-blue-600 dark:border-zinc-600"
                      />
                      {t}
                    </label>
                  ))}
                </div>
              </div>
              {form.userType === 'Client' && (
                <div>
                  <div className="mb-1 flex items-center justify-between">
                    <label className="text-xs font-medium text-zinc-500">Customer (assigned)</label>
                    <button
                      type="button"
                      onClick={loadCustomers}
                      disabled={customersLoading}
                      title="Refresh client list from vwork"
                      className="rounded p-1 text-zinc-500 hover:bg-zinc-200 hover:text-zinc-700 disabled:opacity-50 dark:hover:bg-zinc-700 dark:hover:text-zinc-300"
                    >
                      <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={customersLoading ? 'animate-spin' : ''}>
                        <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
                        <path d="M3 3v5h5" />
                        <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16" />
                        <path d="M16 21h5v-5" />
                      </svg>
                    </button>
                  </div>
                  <select
                    value={form.customer}
                    onChange={(e) => setForm((f) => ({ ...f, customer: e.target.value }))}
                    className="w-full rounded border border-zinc-300 px-2 py-1.5 text-sm dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                  >
                    <option value="">— None —</option>
                    {distinctCustomers.map((c) => (
                      <option key={c} value={c}>{c}</option>
                    ))}
                  </select>
                </div>
              )}
              <div className="flex justify-end gap-2 pt-2">
                <button type="button" onClick={closeForm} className="rounded bg-zinc-200 px-4 py-2 text-sm hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600">
                  Cancel
                </button>
                <button type="submit" className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700">
                  {editing ? 'Update' : 'Create'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
