package services

import "livy/livy/models"

func (s *LivySvc) GetAllConfiguration()([]models.Configuration,error){
	res, err := s.db.GetAllConfiguration(s.ctx)
	if err != nil {
		return []models.Configuration{} , err
	}

	return res, nil
}

func (s *LivySvc) GetConfiguration(configname string)(models.Configuration, error){
	res, err := s.db.GetConfiguration(s.ctx,configname)
	if err != nil {
		return models.Configuration{} , err
	}

	return res, nil
}

func (s *LivySvc) InsertConfiguration(configname,value string) error{
	err := s.db.InsertConfiguration(s.ctx, configname,value)
	if err != nil {
		return err
	}

	return nil
}

func (s *LivySvc) UpdateConfiguration(id,configname,value string) error{
	err := s.db.UpdateConfiguration(s.ctx, configname, value,id)
	if err != nil {
		return err
	}

	return nil
}