/*#1 Ошибка: Для того, чтобы повторное выполнение скрипта не приводило к возникновению
*/ошибки, необходимо проверять существование в БД создаваемого объекта
create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
	-- #2 Ошибка - отсутствие пустых строк между логическими блоками
AS
set nocount on
begin
	-- #22 Ошибка - declare пишется один раз
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	-- #3 Ошибка - рекомендуется не использовать длину поля max
	declare @ErrorMessage varchar(max)

-- Проверка на корректность загрузки
-- #4 Ошибка - отступ комментария должен быть равным отступа строки кода, к которой он относится
	if not exists (
	-- #5 Ошибка - отсутствие табулированного отступа блока, внутри if ()
	select 1
	-- #6 Ошибка в наименовании алиаса, согласно стандарту оно должно быть if (первые две заглавные буквы имени)
	from syn.ImportFile as f
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		-- #7 Ошибка - слишком большой отступ: begin должен находиться на уровне предыдущей скобки
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

			raiserror(@ErrorMessage, 3, 1)
			-- #8 Ошибка - отсутствие пустых строк перед return
			return
		-- #7 Ошибка - слишком большой отступ перед end, аналогично предыдущему begin
		end

	-- #9 Ошибка - отсутствие пробела между -- и текстом комментария
	--Чтение из слоя временных данных
	/*Ошибка 10 - В этой части кода, согласно правилам форматирования SQL, можно написать запрос, используя With, то есть использовать CET
	,что улучшит читаемость кода*/
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		-- #11 Ошибка - Неявное преобразование в типах данных
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	/*#12 Ошибка - согласно стандарту: Алиас обязателен для объекта и задается с помощью ключевого слова  as. 
        В данной строчке as отсутствует перед наименованием алиаса.
	*/
	from syn.SA_CustomerSeasonal cs
		
		-- #13 Ошибка - в этих строчках неявно указан тип JOIN. Следует писать INNER JOIN

		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and cd.ID_mapping_DataSource = 1
		-- #14 Ошибка - перепутан порядок: При соединение двух таблиц, сперва после  on  указываем поле присоединяемой таблицы 
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	/*Ошибка 15 - В этой части кода, так же, согласно правилам форматирования SQL, можно написать запрос, используя With, то есть использовать CET
	,что улучшит читаемость кода*/
	select
		cs.*
		,case
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			-- #16 Ошибка - отсутствие переноса then на новую строку, а так же табулированного отступа, соответственно
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность' 
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	-- #17 Ошибка - опечатка, неправильное название алиаса - cc
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null


	-- Обработка данных из файла
	/* Ошибка 23 - согласно правилам конструкции merge, into писать необязательно, поэтому, 
	для облегчения чтения кода, лучше его убрать*/
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		-- Ошибка 18 - если мы изменим код, используя CTE, то мы больше не можем пользоваться этой временной таблицей 
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched 
		-- #Ошибка 19 - алиас t не объявлен
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		-- #20 Ошибка - не хватает табулированного отступа перед update, в конструкции then update
		set
		update
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		-- #21 Ошибка - эта строка слишком длинная и ее лучше разделить, т.к ее длина затрудняет прочтение
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive)
	;

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Дата начала'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end

end
