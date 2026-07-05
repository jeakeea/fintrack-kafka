-- Демо-счета и стартовые балансы (как «открывающие» проводки credit).

INSERT INTO accounts (id, owner_name, currency) VALUES
    ('acc_ivan',  'Иван Смирнов',  'RUB'),
    ('acc_maria', 'Мария Петрова', 'RUB'),
    ('acc_oleg',  'Олег Сидоров',  'RUB')
ON CONFLICT (id) DO NOTHING;

-- Стартовые балансы: Иван 10 000.00, Мария 5 000.00, Олег 5 000.00 (в копейках)
INSERT INTO ledger_entries (payment_id, account_id, direction, amount) VALUES
    ('seed_ivan',  'acc_ivan',  'credit', 1000000),
    ('seed_maria', 'acc_maria', 'credit', 500000),
    ('seed_oleg',  'acc_oleg',  'credit', 500000)
ON CONFLICT DO NOTHING;
