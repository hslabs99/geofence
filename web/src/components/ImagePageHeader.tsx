export default function ImagePageHeader({
  title,
  src = '/images/header.png',
  /**
   * Raw mode shows the image at its natural pixel size (no scaling).
   * This can overflow the container, so the header becomes scrollable.
   */
  raw = true,
  /** Only used when raw=false. */
  heightClassName = 'h-10 sm:h-12',
}: {
  title: string;
  src?: string;
  raw?: boolean;
  heightClassName?: string;
}) {
  return (
    <div className={`relative w-full ${raw ? 'overflow-auto' : `overflow-hidden ${heightClassName}`}`}>
      {raw ? (
        <img src={src} alt="" className="block max-w-none" />
      ) : (
        // Keep the old cover behavior for non-raw usage.
        <img src={src} alt="" className="h-full w-full object-cover object-left" />
      )}

      <div className="absolute inset-0 bg-zinc-950/40" aria-hidden="true" />
      <div className="absolute inset-0 flex items-center justify-center px-4">
        <h1 className="text-center text-3xl font-semibold tracking-tight text-white drop-shadow sm:text-4xl">
          {title}
        </h1>
      </div>
    </div>
  );
}

