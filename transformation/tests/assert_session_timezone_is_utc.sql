{{ config(severity='error') }}

/*
    The session timezone MUST be UTC.

    DuckDB inherits the machine timezone unless told otherwise. When it does,
    `extract(hour FROM a_timestamptz)` returns LOCAL hours while looking exactly
    like it returns UTC hours - so every date and hour-of-day derivation is
    machine-dependent.

    This was not hypothetical: it was found on a developer machine set to
    Asia/Calcutta, where the untouched code accidentally produced the right IST
    answer and would have produced UTC hours in CI. The morning peak would have
    appeared at 02:00 and looked plausible.

    Pinned in profiles.yml. Asserted here so it cannot regress silently.
*/

select current_setting('TimeZone') as session_timezone
where current_setting('TimeZone') != 'UTC'
